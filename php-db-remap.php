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

interface Writer {
	public function openTable($table, $config);
	public function writeRow(array $row);
	public function closeTable();
	public function discardCopy();
}

interface ValueProvider {
	public function init(SimpleLogger $logger, PDOConnection $pdo);
	public function getMarker();
	public function getValue($key, $row);
}

class PDOConnection {
	public function __construct(SimpleLogger $logger, $dsn, $user, $pass) {
        try {
            $this->conn = new PDO($dsn, $user, $pass);
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

class SQLValueProvider implements ValueProvider {
	public function init(SimpleLogger $logger, PDOConnection $pdo) {
		$this->logger = $logger;
		$this->conn = $pdo->get();
	}
	
	public function getMarker() {
		return $this->marker;
	}


}

class StableValueProvider implements ValueProvider {
	public function __construct($originalTable, $key, $stableKey) {
		$this->targetTable = $targetTable;
		$this->key = $key;
		$this->stableKey = $stableKey;
		$this->values = array();
		$this->maxValue = 0;	

		$this->_loadAll();
	}

	public function init(SimpleLogger $logger, PDOConnection $pdo) {
		$this->conn = $pdo->get();
		$this->logger = $logger;
	}

	public function getValue($stableValue) {
		if (array_key_exists($stableValue, $this->values)) {
			return $this->values[$stableValue];
		} else {
			$this->maxValue++;
			return $this->maxValue;
		}
	}

	protected function _loadAll() {
		$stmt = $this->conn->prepare(sprintf('SELECT %s, %s FROM %s', $this->originalTable, $this->key, $this->stableKey));

        try {
            $stmt->execute();
        } catch (PDOException $e) {
            $this->logger->fatal(sprintf("Cannot fetch stable values on %s: %s", $this->originalTable, $e->getMessage()));
            return FALSE;
        }

        $fields = $stmt->fetchAll(PDO::FETCH_ASSOC);
	
		foreach ($fields as $key => $val) {
			$this->values[$val[$this->stableKey]] = $val[$this->key];
			
			$value = intval($val[$this->key]);
			if ($value > $this->maxValue) {
				$this->maxValue = $value;
			}
		}

		var_dump($this->values);exit;
		return TRUE;
	}
}

class PDOWriter implements Writer {
	public function __construct(SimpleLogger $logger, PDOConnection $conn) {
		$this->conn = $conn->get();
		$this->stmt = FALSE;
		$this->logger = $logger;

		try {	
			$this->conn->exec('SET NAMES utf8');
		} catch (PDOException $e) {
			$this->logger->fatal("Cannot set NAMES to utf8 while opening MySQL database");
			exit;
		}
	}

	public function setStableTable($tableName) {
		$this->stableTableName = $tableName;
	}

	public function openTable($tableName, $config) {
		$this->tableName = $tableName;
		$this->config = $config;
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

	public function query($sql, $error) {
        try {
            $this->conn->query($sql);
        } catch (PDOException $e) {
            $this->logger->log('error', $error . $e->getMessage());
            return FALSE;
        }
        return TRUE;
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

		$this->flippedFields = array();

		if (count($this->config) > 0) {
			// Here SQL: values might override each other. But we won't use them...
			$flippedFields = array_flip($this->config);

			foreach ($flippedFields as $k => $v) {
				if (substr($k, 0, 3) === 'SQL:') {
					continue;
				}

				$split = explode('->', $k, 2);

				if (count($split) > 1) {
					$k = trim($split[0]);
				}

				$this->flippedFields[$k] = $v;
			}

		} else {
			foreach ($fields as $field) {
				$this->flippedFields[$field['Field']] = $field['Field'];
			}
		}

		$this->fields = array();

		foreach ($fields as $field) {
			$key = $field['Field'];

			if (array_key_exists($key, $this->config)) {
				$this->fields[$key] = $field['Type'];
			}
		}

		return TRUE;
	}

	protected function _getMappedKey($key, $flippedConfig) {
		foreach ($flippedConfig as $name => $val) {
			if ($key === $name || substr($name, 0, strlen($key)+2) === ($key.', ')) {
				return $val;
			}
		}

		return FALSE;
	}

	protected function _getMappedKeys(array $keys) {
		if (count($this->config) == 0) {
			return $keys;
		}

		$result = array();

		foreach ($this->config as $key => $val) {
			if (strpos($val, 'SQL:') === 0 || strpos($val, 'STABLE:')) {
				$result[] = $key;
			}
		}

		$flippedConfig = array_flip($this->config);

		foreach ($keys as $key) {
			$val = $this->_getMappedKey($key, $flippedConfig);
			if ($val !== FALSE) {
				$result[] = $val;
			}
		}

		return $result;
	}

	protected function _getMappedValues(array $keys) {
		$values = array();

		foreach ($keys as $key) {
			// Only include mapped fields.
			if (array_key_exists($key, $this->config)) {
				if (strpos($this->config[$key], 'SQL:') === 0) {
					$values[] = substr($this->config[$key], 4);
				} else {
					$split = explode('->', $this->config[$key], 2);

					if (count($split) > 1 && strlen($split[1]) > 0) {
						$values[] = trim($split[1]);
					} else {
						$values[] = '?';
					}
				}
			}	
		}

		return $values;
	}

	protected function _prepareStmt(array $keys) {
		$this->mappedKeys = $this->_getMappedKeys($keys);

		$names = implode(', ', $this->mappedKeys);
		$values = implode(', ', $this->_getMappedValues($this->mappedKeys));
		$stmt = sprintf('INSERT INTO %s (%s) VALUES (%s)', $this->tableName, $names, $values);

		try {
			$this->stmt = $this->conn->prepare($stmt);
		} catch (PDOException $e) {
			$this->logger->fatal(sprintf("Cannot create prepared statement: %s", $e->getMessage()));
			return FALSE;
		}
		return TRUE;
	}

	protected function _getSortedValues($values) {
		$result = array();

		foreach ($values as $oldName => $val) {
			if (array_key_exists($oldName, $this->flippedFields)) {
				$result[$this->flippedFields[$oldName]] = $val;
			}
		}

		$finalValues = array();

		foreach ($this->mappedKeys as $key) {
			if (array_key_exists($key, $result)) {
				$finalValues[] = $this->_formatValue($key, $result[$key]);
			}
		}

		return $finalValues;
	}

	protected function _insert(array $row) {
		$values = $this->_getSortedValues($row);
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

	protected function _formatValue($key, $val) {
		if (!array_key_exists($key, $this->fields)) {
			$this->logger->fatal(sprintf("Field %s does not seem to be set in MySQL destination DB (table %s).", $key, $this->tableName));
			return FALSE;
		}
		$type = $this->fields[$key];
		$value = $val;

		switch ($type) {
		case 'date':
			$value = date('d-m-Y', strtotime($val));
			break;
		default:
			// Avoid null values. XXX: Do this only on NOT NULL?
			if ($val === null) {
				$value = '';
			}
		}
		// TODO: Convert int(*) and float/double too?

		return $value;
	}
}

class PDOWriterAtomic implements Writer {
	public function __construct(SimpleLogger $logger, PDOConnection $conn) {
		$this->conn = $conn->get();
		$this->logger = $logger;
		$this->writer = new PDOWriter($logger, $conn);
		$this->keptConf = array();
	}

	public function openTable($tableName, $config) {
		$this->tableName = $tableName;
		$this->tmpTable = $tableName . '_copy';

		if (!$this->_tableExists($this->tableName)) {
			$this->logger->fatal("Table {$this->tableName} doesn't exist.");
			return FALSE;
		}

		if ($this->_tableExists($this->tmpTable)) {
			$this->logger->fatal("Table {$this->tmpTable} already exists.");
			return FALSE;
		}

		$res = $this->conn->exec(sprintf('CREATE TABLE %s LIKE %s', $this->tmpTable, $tableName));
		if ($res === FALSE) {
			$this->logger->fatal("Couldn't create table {$this->tmpTable}");
			return FALSE;
		}

		return $this->writer->openTable($this->tmpTable, $config);
	}

	public function writeRow(array $row) {
		return $this->writer->writeRow($row);
	}

	public function discardCopy() {
		return $this->_dropTable($this->tmpTable);
	}

	public function closeTable() {
		$this->_openTransaction();

		if (count($this->keptConf) > 1) {
			if (!$this->_restoreKeptData($this->keptConf)) {
				$this->_cancelTransaction();
				return FALSE;
			}
		}
		
		if (!$this->writer->closeTable()) {
			$this->_cancelTransaction();
			return FALSE;
		}

		try {
			$this->conn->query(sprintf('TRUNCATE %s', $this->tableName));
			$this->conn->query(sprintf('INSERT INTO %s SELECT * FROM %s', $this->tableName, $this->tmpTable));
		} catch (PDOException $e) {
			$this->_cancelTransaction();
			$this->logger->log('error', sprintf("Couldn't commit transaction to update table %s: %s", $this->tableName, $e->getMessage()));
			$this->_dropTable($this->tmpTable);
			return FALSE;
		}

		$this->_closeTransaction();

		$res = $this->_dropTable($this->tmpTable);
		$this->conn->query(sprintf('OPTIMIZE TABLE %s', $this->tableName));
		return $res;
	}

	public function setKeptData($config) {
		$this->keptConf = $config;
	}

	protected function _restoreKeptData($config) {
		if (count($config) == 0) {
			return TRUE;
		}

		$key = $config['key'];
		$fields = $config['fields'];

		if ($key == '' || count($fields) == 0) {
			return TRUE;
		}

		$data = $this->_loadKeptData($this->tableName, $key, $fields);
		if ($data === FALSE) {
			return FALSE;
		}
	   
		return $this->_updateKeptData($this->tmpTable, $key, $data);
	}

	protected function _openTransaction() {
		try {
			$this->conn->beginTransaction();
		} catch (PDOException $e) {
			$this->logger->log('error', sprintf('Cannot start transaction: %s', $e->getMessage()));
			return FALSE;
		}

		return TRUE;
	}

	protected function _closeTransaction() {
		try {
			$this->conn->commit();
		} catch (PDOException $e) {
			$this->logger->log('error', sprintf('Cannot commit transaction: %s', $e->getMessage()));
			$this->_cancelTransaction();
			return FALSE;
		}

		return TRUE;
	}

	protected function _cancelTransaction() {
		try {
			$this->conn->rollBack();
		} catch (PDOException $e) {
			$this->logger->fatal(sprintf('Cannot rollback transaction: %s', $e->getMessage()));
			return FALSE;
		}

		return TRUE;
	}

	protected function _getMarkerFields($updateKey, $data) {
		$data = current($data);
		$markers = array();

		foreach ($data as $key => $val) {
			if ($key !== $updateKey) {
				$markers[] = sprintf("%s = ?", $key);
			}
		}

		return implode(', ', $markers);
	}

	protected function _updateKeptData($tableName, $key, $data) {
		$stmt = $this->conn->prepare(sprintf('UPDATE %s SET %s WHERE %s = ?',  $tableName, $this->_getMarkerFields($key, $data), $key));

		foreach ($data as $whereKey => $values) {
			$cleanValues = array();

			foreach($values as $k => $val) {
				if ($k !== $key) {
					$cleanValues[] = $val;
				}
			}

			$cleanValues[] = $values[$key];

			try {
				$stmt->execute($cleanValues);
			} catch (PDOException $e) {
				$this->logger->fatal(sprintf("Cannot update table %s: %s", $this->tableName, $e->getMessage()));
				return FALSE;
			}
		}

		return TRUE;
	}

	protected function _loadKeptData($tableName, $key, $fields) {
		$data = array();

		$stmt = $this->conn->prepare(sprintf('SELECT %s,%s FROM %s LOCK IN SHARE MODE', $key, implode(', ', $fields), $tableName));

        try {
            $stmt->execute();
        } catch (PDOException $e) {
            $this->logger->fatal(sprintf("Cannot get kept data from table %s: %s", $this->tableName, $e->getMessage()));
            return FALSE;
        }
        $fields = $stmt->fetchAll(PDO::FETCH_ASSOC);
		
		foreach ($fields as $val) {
			if (array_key_exists($key, $val)) {
				$data[$val[$key]] = $val;
			}
		}

		return $data;
	}

	protected function _tableExists($tableName) {
		try {
			$this->conn->query(sprintf("SELECT 1 FROM %s", $tableName));
		} catch (PDOException $e) {
			return FALSE;
		}
		return TRUE;
	}

	protected function _renameTable($from, $to) {
		return $this->writer->query(sprintf("RENAME TABLE %s TO %s", $from, $to),
			sprintf("Cannot rename table %s to %s: ", $from, $to));		
	}

	protected function _dropTable($tableName) {
		return $this->writer->query(sprintf('DROP TABLE %s', $tableName),
			sprintf("Cannot drop table %s: ", $tableName));		
	}
}

class PDOReader {
	public function __construct(SimpleLogger $logger, PDOConnection $conn) {
		$this->logger = $logger;
		$this->conn = $conn->get();
	}

	public function copyTableInto($tableName, Writer $dest) {
		try {
			$stmt = $this->conn->query(sprintf('SELECT * FROM %s', $tableName));
		} catch (PDOException $e) {
			$this->logger->fatal(sprintf("Cannot select all from table %s: %s", $this->tableName, $e->getMessage()));
			return FALSE;
		}
		
		do {
			$row = $stmt->fetch(PDO::FETCH_ASSOC);

			if ($row === FALSE) {
				break;
			}

			if (!$dest->writeRow($row)) {
				$this->logger->fatal('Error while copying. Copying halted');
				$stmt->closeCursor();
				return FALSE;
			}
		} while (TRUE);
		
		return TRUE;
	}

	public function close() {
		return TRUE;
	}
}

class Configuration {
	public function __construct() {
		$this->conf = array();
	}

	public function load($filename) {
		$conf = parse_ini_file($filename, TRUE);
		if ($conf === FALSE) {
			die('Cannot parse given .ini configuration');
		}

		$this->conf = array_merge_recursive($this->conf, $conf);
	}

	public function getWriter() {
		if (!array_key_exists('writer', $this->conf)) {
			die('You must specify a writer section in config'."\n");
		}
		$w = $this->conf['writer'];

		foreach(array('user', 'password', 'dsn') as $key) {
			if (!array_key_exists($key, $w)) {
				die(sprintf("Please set required %s in writer section\n", $key));
			}
		}

		return $w;
	}

	public function getReader() {
		if (!array_key_exists('reader', $this->conf)) {
			die('You must specify a reader section in config'."\n");
		}
		$r = $this->conf['reader'];
	
		foreach(array('user', 'password', 'dsn') as $key) {
			if (!array_key_exists($key, $r)) {
				die(sprintf("Please set required %s in reader section\n", $key));
			}
		}

		return $r;
	}

	public function getTables() {
		if (!array_key_exists('tables', $this->conf)) {
			die('Please set required tables section in configuration'."\n");
		}
		if (!is_array($this->conf['tables'])) {
			die('Tables list must be an array'."\n");
		}
		return array_flip($this->conf['tables']);
	}

	public function getTable($tableName) {
		if (!array_key_exists($tableName, $this->conf)) {
			return array();
		}

		return $this->conf[$tableName];
	}

	public function getTableClass($tableName, $class) {
		return $this->getTable(sprintf('%s:%s', $tableName, $class));
	}
}

function usage() {
	die("Usage: php-oci-import.php <ini-file...> [<log-file>]\n");
}

function loadConfiguration(array $argv, Configuration $conf) {
	$logfile = '';

	for ($i = 1; $i < count($argv); $i++) {
		if (substr($argv[$i], -4) === '.ini') {
			$conf->load($argv[$i]);
		} else {
			// Logfile set twice.
			if ($logfile !== '') {
				usage();
			}

			$logfile = $argv[$i];
		}
	}

	return $logfile;
}

function copyTable($fromTable, $toTable, PDOReader $reader, PDOWriterAtomic $writer, Configuration $conf) {
	if (!$writer->openTable($toTable, $conf->getTable($toTable))) {
		return FALSE;
	}

	$res = $reader->copyTableInto($fromTable, $writer);
	if ($res === FALSE) {
		$writer->discardCopy();
		return FALSE;
	}

	$writer->setKeptData($conf->getTableClass($toTable, 'keep'));

	if (!$writer->closeTable()) {
		$writer->discardCopy();
		// Continue if importing into one table failed.
		return FALSE;
	}

	return TRUE;
}

function main() {
	global $argv;

	$conf = new Configuration();
	$logfile = loadConfiguration($argv, $conf);
	$logger = new SimpleLogger($logfile);

	$w = $conf->getWriter();
	$r = $conf->getReader();

	$readerConn = new PDOConnection($logger, $r['dsn'], $r['user'], $r['password']);
	$writerConn = new PDOConnection($logger, $w['dsn'], $w['user'], $w['password']);
	
	$reader = new PDOReader($logger, $readerConn);
	$writer = new PDOWriterAtomic($logger, $writerConn);

	$tables = $conf->getTables();

	foreach ($tables as $fromTable => $toTable) {
		$logger->log('info', sprintf('Starting import of table %s into %s', $fromTable, $toTable));

		if (copyTable($fromTable, $toTable, $reader, $writer, $conf)) {
			$logger->log('info', sprintf('Import of table %s into %s finished', $fromTable, $toTable));
		} else {
			$logger->log('info', sprintf('Import of table %s into %s failed', $fromTable, $toTable));
		}
	}

	$reader->close();
	return TRUE;
}

// Behave nicely on the shell.
exit (main() ? 0 : 1);
