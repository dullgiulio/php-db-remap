#!/usr/bin/env php
<?php

class SimpleLogger {
	public function __construct($output) {
		$this->output = $output;
	}

	public function log($level, $message) {
		$line = sprintf("[%s] %s: %s\n",  date('Y-m-d H:i:s'), strtoupper($level), $message);
		if ($this->output == '') {
			echo $line;
		} else {
			file_put_contents($this->output, $line, FILE_APPEND);
		}
	}

	public function fatal($message) {
		$this->log('fatal', $message);
		echo 'FATAL: ' . $message . "\n";
	}
}

interface CopyWriter {
	public function openTable($table);
	public function writeRow(array $row);
	public function closeTable();
	public function discardCopy();
}

class MysqlConnection {
	public function __construct(SimpleLogger $logger, $user, $pass, $host, $db) {
        try {
            $this->conn = new PDO(sprintf('mysql:host=%s;dbname=%s', $host, $db), $user, $pass);
        } catch (PDOException $e) {
			$logger->fatal($e->getMessage());
			exit;
		}

		$this->conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
	}

	public function get() {
		return $this->conn;
	}
}

class MysqlCopyWriter implements CopyWriter {
	public function __construct(SimpleLogger $logger, PDO $conn) {
		$this->conn = $conn;
		$this->stmt = FALSE;
		$this->logger = $logger;

		try {	
			$this->conn->exec('SET NAMES utf8');
		} catch (PDOException $e) {
			$this->logger->fatal("Cannot set NAMES to utf8 while opening MySQL database");
			exit;
		}
	}

	public function openTable($tableName) {
		$this->tableName = $tableName;
		return $this->_loadTableFields();
	}

	public function writeRow(array $row) {
		if ($this->stmt === FALSE) {
			$this->_prepareStmt(array_keys($row));	
		}

		return $this->_insert($row);
	}

	public function closeTable() {
		$this->stmt = FALSE;
		$this->fields = FALSE;
		return TRUE;
	}

	public function discardCopy() {
		return FALSE;
	}

	protected function _loadTableFields() {
		$stmt = $this->conn->prepare(sprintf('DESCRIBE %s', $this->tableName));

		try {
			$stmt->execute();
		} catch (PDOException $e) {
			$this->logger->fatal(sprintf("Cannot describe table %s: %s", $this->tableName, $e->getMessage()));
			return FALSE;
		}
		$fields = $stmt->fetchAll(PDO::FETCH_ASSOC);
		
		foreach ($fields as $field) {
			$this->fields[$field['Field']] = $field['Type'];
		}

		return TRUE;
	}

	protected function _prepareStmt(array $keys) {
		$markers = rtrim(rtrim(str_repeat('?, ', count($keys)), ' '), ',');
		$values = implode(', ', $keys);
		$stmt = sprintf('INSERT INTO %s (%s) VALUES (%s)', $this->tableName, $values, $markers);

		try {
			$this->stmt = $this->conn->prepare($stmt);
		} catch (PDOException $e) {
			$this->logger->fatal(sprintf("Cannot create prepared statement: %s", $e->getMessage()));
			return FALSE;
		}
		return TRUE;
	}

	protected function _insert(array $row) {
		$values = $this->_formatValues($row);
		if ($values === FALSE) {
			return FALSE;
		}

		try {
			$res = $this->stmt->execute(array_values($values));
		} catch (PDOException $e) {
			$this->logger->fatal(sprintf("Cannot execute insert statement into table %s: %s", $this->tableName, $e->getMessage()));
			return FALSE;
		}
		return TRUE;
	}

	protected function _formatValues(array $row) {
		$res = array();

		foreach ($row as $key => $val) {
			if (!isset($this->fields[$key])) {
				$this->logger->fatal(sprintf("Field %s does not seem to be set in MySQL destination DB (table %s).", $key, $this->tableName));
				return FALSE;
			}
			$type = $this->fields[$key];
			$value = $val;

			if ($type == 'date') {
				$value = date('Y-m-d', strtotime($val));
			}
			// TODO: Convert int(*) and float/double too?

			$res[$key] = $value;
		}
		
		return $res;
	}
}

class MysqlCopyWriterAtomic implements CopyWriter {
	public function __construct(SimpleLogger $logger, PDO $conn) {
		$this->conn = $conn;
		$this->logger = $logger;
		$this->mysql = new MysqlCopyWriter($logger, $conn);
	}

	public function openTable($tableName) {
		$this->tableName = $tableName;
		$this->tmpTable = $tableName . '_copy';

		if ($this->_tableExists($this->tmpTable)) {
			$this->logger->fatal("Table {$this->tmpTable} already exists.");
			return FALSE;
		}

		$res = $this->conn->exec(sprintf('CREATE TABLE %s LIKE %s', $this->tmpTable, $tableName));
		if ($res === FALSE) {
			$this->logger->fatal("Couldn't create table {$this->tmpTable}");
			return FALSE;
		}

		return $this->mysql->openTable($this->tmpTable);
	}

	public function writeRow(array $row) {
		return $this->mysql->writeRow($row);
	}

	public function discardCopy() {
		return $this->_dropTable($this->tmpTable);
	}

	public function closeTable() {
		if (!$this->mysql->closeTable()) {
			return FALSE;
		}
	
		$randName = sprintf("%s_%04d", $this->tmpTable, rand(0, 1000));
		if (!$this->_renameTable($this->tableName, $randName)) {
			$this->_dropTable($this->tmpTable);
			$this->logger->fatal("Cannot rename table to temporary name");
			return FALSE;
		}

		if (!$this->_renameTable($this->tmpTable, $this->tableName)) {
			if (!$this->_renameTable($randName, $this->tableName)) {
				$this->logger->log('error', "Couldn't rename temporary named old data table into real table");
			}
			$this->_dropTable($this->tmpTable);
			$this->logger->fatal("Failed to rename table to real name");
			return FALSE;
		}

		return $this->_dropTable($randName);
	}

	protected function _tableExists($tableName) {
		try {
			$this->conn->exec(sprintf("SELECT 1 FROM %s", $tableName));
		} catch (PDOException $e) {
			return FALSE;
		}
		return TRUE;
	}

	protected function _renameTable($from, $to) {
		try {
			$this->conn->exec(sprintf("RENAME TABLE %s TO %s", $from, $to));
		} catch (PDOException $e) {
			$this->logger->log('error', sprintf("Cannot rename table %s to %s: %s", $from, $to, $e->getMessage()));
			return FALSE;
		}
		return TRUE;
	}

	protected function _dropTable($tableName) {
		try {
			$this->conn->exec(sprintf('DROP TABLE %s', $tableName));
		} catch (PDOException $e) {
			$this->logger->log('error', sprintf("Cannot drop table %s: %s", $tableName, $e->getMessage()));
			return FALSE;
		}
		return TRUE;
	}
}

class OciReader {
	public function __construct(SimpleLogger $logger, $user, $pass, $service) {
		$this->logger = $logger;
		$this->conn = oci_connect($user, $pass, $service);
		if (!$this->conn) {
    		$e = oci_error();
			$this->logger->fatal($e['message']);
			exit;
		}
	}

	public function copyTableInto($tableName, CopyWriter $dest) {
		if (!$this->conn) {
			$this->logger->fatal("Invalid Oracle reader connection");
			return FALSE;
		}
		
		$stid = oci_parse($this->conn, sprintf('SELECT * FROM %s', $tableName));
		if (!$stid) {
			$e = oci_error($this->conn);
			$this->logger->fatal($e['message']);
			return FALSE;
		}

		$r = oci_execute($stid);
		if (!$r) {
			$e = oci_error($stid);
			$this->logger->fatal($e['message']);
			return FALSE;
		}

		while ($row = oci_fetch_array($stid, OCI_ASSOC+OCI_RETURN_NULLS)) {
			if (!$dest->writeRow($row)) {
				$this->logger->fatal("Error while copying. Copying halted");
				oci_free_statement($stid);
				return FALSE;
			}
		}

		oci_free_statement($stid);
		return TRUE;
	}

	public function close() {
		oci_close($this->conn);
	}
}

class Configuration {
	public function __construct($filename) {
		$this->conf = parse_ini_file($filename, TRUE);
		if ($this->conf === FALSE) {
			die('Cannot parse given .ini configuration');
		}
	}

	public function getWriter() {
		if (!isset($this->conf['writer'])) {
			die('You must specify a writer section in config'."\n");
		}
		$w = $this->conf['writer'];
		$type = 'mysql';

		if (isset($w['type'])) {
			$type = $w['type'];
		}

		if ($type != 'mysql') {
			die('Only allowed writer type is Mysql'."\n");
		}

		foreach(array('user', 'password', 'host', 'database') as $key) {
			if (!isset($w[$key])) {
				die(sprintf("Please set required %s in writer section\n", $key));
			}
		}

		return $w;
	}

	public function getReader() {
		if (!isset($this->conf['reader'])) {
			die('You must specify a reader section in config'."\n");
		}
		$r = $this->conf['reader'];
		$type = 'oracle';

		if (isset($w['type'])) {
			$type = $w['type'];
		}

		if ($type != 'oracle') {
			die('Only supported reader is Oracle'."\n");
		}
	
		foreach(array('user', 'password', 'service') as $key) {
			if (!isset($r[$key])) {
				die(sprintf("Please set required %s in reader section\n", $key));
			}
		}

		return $r;
	}

	public function getTables() {
		if (!isset($this->conf['tables'])) {
			die('Please set required tables section in configuration'."\n");
		}
		if (!is_array($this->conf['tables'])) {
			die('Tables list must be an array'."\n");
		}
		return array_values($this->conf['tables']);
	}

}

function main() {
	global $argv;

	if (count($argv) < 2) {
		die("Usage: php-oci-import.php <ini-file> [<log-file>]\n");
	}
	
	$conf = new Configuration($argv[1]);
	$logfile = '';

	if (isset($argv[2])) {
		$logfile = $argv[2];
	}

	$logger = new SimpleLogger($logfile);

	$w = $conf->getWriter();
	$r = $conf->getReader();

	$mysqlConn = new MysqlConnection($logger, $w['user'], $w['password'], $w['host'], $w['database']);
	$reader = new OciReader($logger, $r['user'], $r['password'], $r['service']);
	$writer = new MysqlCopyWriterAtomic($logger, $mysqlConn->get());

	$tables = $conf->getTables();

	foreach ($tables as $table) {
		$logger->log('info', sprintf('Starting copy of table %s', $table));

		if (!$writer->openTable($table)) {
			return FALSE;
		}

		$res = $reader->copyTableInto($table, $writer);
		if ($res === FALSE) {
			$writer->discardCopy();
			continue;
		}

		if (!$writer->closeTable()) {
			// Continue if a importing into one table failed.
			continue;
		}
		
		$logger->log('info', sprintf('Copy of table %s finished', $table));
	}

	$reader->close();
	return TRUE;
}

// Behave nicely on the shell.
exit (main() ? 0 : 1);
