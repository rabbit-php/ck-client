<?php

namespace OneCk;

use Rabbit\Base\Contract\InitInterface;

class Client implements InitInterface
{
    /**
     * @var resource
     */
    private $conn;

    /**
     * @var Write
     */
    private Write $write;

    /**
     * @var Read
     */
    private Read $read;

    /**
     * @var Types
     */
    private Types $types;

    const NAME          = 'PHP-ONE-CLIENT';
    const VERSION_MAJOR = 2;
    const VERSION_MINOR = 1;

    const DBMS_MIN_V_TEMPORARY_TABLES         = 50264;
    const DBMS_MIN_V_TOTAL_ROWS_IN_PROGRESS   = 51554;
    const DBMS_MIN_V_BLOCK_INFO               = 51903;
    const DBMS_MIN_V_CLIENT_INFO              = 54032;
    const DBMS_MIN_V_SERVER_TIMEZONE          = 54058;
    const DBMS_MIN_V_QUOTA_KEY_IN_CLIENT_INFO = 54060;

    const DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE = 54337;
    const DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME      = 54372;
    const DBMS_MIN_REVISION_WITH_VERSION_PATCH            = 54401;
    const DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE     = 54405;
    const DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA = 54410;
    const DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO        = 54420;
    const DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS = 54429;
    const DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET       = 54441;
    const DBMS_MIN_REVISION_WITH_OPENTELEMETRY            = 54442;
    const DBMS_MIN_REVISION_WITH_DISTRIBUTED_DEPTH        = 54448;
    const DBMS_MIN_REVISION_WITH_INITIAL_QUERY_START_TIME = 54449;
    const DBMS_MIN_REVISION_WITH_INCREMENTAL_PROFILE_EVENTS = 54451;
    const VERSION = self::DBMS_MIN_REVISION_WITH_INCREMENTAL_PROFILE_EVENTS;

    public function __construct(public readonly string $dsn = 'tcp://127.0.0.1:9000', public readonly string $username = 'default', public readonly string $password = '', public readonly string $database = 'default', public readonly array $options = [])
    {
    }

    public function init(): void
    {
        $context = isset($this->options['tcp_nodelay']) && !empty($this->options['tcp_nodelay']) ? stream_context_create(
            ['socket' => ['tcp_nodelay' => true]]
        ) : null;

        $flags = isset($this->options['persistent']) && !empty($this->options['persistent']) ?
            STREAM_CLIENT_CONNECT | STREAM_CLIENT_PERSISTENT
            : STREAM_CLIENT_CONNECT;

        $this->conn = $context ? stream_socket_client(
            $this->dsn,
            $code,
            $msg,
            $options['connect_timeout'] ?? 3,
            $flags,
            $context
        ) : stream_socket_client(
            $this->dsn,
            $code,
            $msg,
            $options['connect_timeout'] ?? 3,
            $flags
        );

        if (!$this->conn) {
            throw new CkException($msg, $code);
        }

        stream_set_timeout($this->conn, $options['socket_timeout'] ?? 30);
        $this->write = new Write($this->conn);
        $this->read  = new Read($this->conn);
        $this->types = new Types($this->write, $this->read);
        $this->hello();
    }

    public function __destruct()
    {
        if (is_resource($this->conn)) {
            stream_socket_shutdown($this->conn, STREAM_SHUT_RDWR);
            fclose($this->conn);
        }
    }

    private function addClientInfo(): void
    {
        $this->write->string(self::NAME)->number(self::VERSION_MAJOR, self::VERSION_MINOR, self::VERSION);
    }

    private function hello(): bool
    {
        $this->write->number(Protocol::CLIENT_HELLO);
        $this->addClientInfo();
        $this->write->string($this->database, $this->username, $this->password);
        $this->write->flush();
        return $this->receive();
    }

    /**
     * @return bool
     */
    public function ping(): bool
    {
        $this->write->number(Protocol::CLIENT_PING);
        $this->write->flush();
        return $this->receive();
    }

    private array $_server_info = [];
    private array $_row_data    = [];
    private int $_total_row   = 0;
    private array $fields       = [];
    /**
     * @return array|bool
     */
    private function receive(): array|bool
    {
        $this->_row_data  = [];
        $this->_total_row = 0;
        $this->fields     = [];
        $_progress_info   = [];
        $_profile_info    = [];

        $code = null;
        do {
            if ($code === null) {
                $code = $this->read->number();
            }
            switch ($code) {
                case Protocol::SERVER_HELLO:
                    $this->setServerInfo();
                    return true;
                case Protocol::SERVER_EXCEPTION:
                    $this->readErr();
                    break;
                case Protocol::SERVER_DATA:
                case Protocol::SERVER_PROFILEEVENTS:
                case Protocol::SERVER_LOG:
                    $n = $this->readData($code);
                    if ($n === null) {
                        break;
                    }
                    if ($n > 1) {
                        $code = $n;
                    }
                    continue 2;
                case Protocol::SERVER_PROGRESS:
                    $_progress_info = [
                        'rows'       => $this->read->number(),
                        'bytes'      => $this->read->number(),
                        'total_rows' => $this->gtV(self::DBMS_MIN_V_TOTAL_ROWS_IN_PROGRESS) ? $this->read->number() : 0,
                        'written_rows' => $this->gtV(self::DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO)  ? $this->read->number() : 0,
                        'written_bytes' => $this->gtV(self::DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO)  ? $this->read->number() : 0,
                    ];
                    break;
                case Protocol::SERVER_END_OF_STREAM:
                    return $this->_row_data;
                    //                    return [
                    //                        'total_row'     => $this->_total_row,
                    //                        'data'          => $this->_row_data,
                    //                        'field'         => $this->fields,
                    //                        'progress_info' => $_progress_info,
                    //                        'profile_info'  => $_profile_info,
                    //                    ];
                case Protocol::SERVER_PROFILE_INFO:
                    $_profile_info = [
                        'rows'                         => $this->read->number(),
                        'blocks'                       => $this->read->number(),
                        'bytes'                        => $this->read->number(),
                        'applied_limit'                => $this->read->number(),
                        'rows_before_limit'            => $this->read->number(),
                        'calculated_rows_before_limit' => $this->read->number()
                    ];
                    break;
                case Protocol::SERVER_TOTALS:
                case Protocol::SERVER_EXTREMES:
                    throw new CkException('Report to me this error ' . $code, CkException::CODE_UNDO);
                    break;
                case Protocol::SERVER_PONG:
                    return true;
                case Protocol::SERVER_TABLECOLUMNS:
                    $this->read->number();
                    $this->read->number();
                    break;
                default:
                    throw new CkException('undefined code ' . $code, CkException::CODE_UNDEFINED);
            }
            $code = null;
        } while (true);
    }

    private function gtV(int $v): bool
    {
        return $this->_server_info['version'] >= $v;
    }


    /**
     * @return array
     */
    public function getServerInfo(): array
    {
        return $this->_server_info;
    }


    private function readData(int $pCode): null|int
    {
        $this->read->number();
        [$row_count, $col_count] = $this->readHeader();
        for ($i = 0; $i < $col_count; $i++) {
            $f   = $this->read->string();
            $t   = $this->read->string();
            if ($pCode === Protocol::SERVER_DATA) {
                $this->fields[$f] = $t;
            }
            if ($row_count > 0) {
                $col = $this->types->unpack($t, $row_count);
                if ($pCode === Protocol::SERVER_DATA) {
                    foreach ($col as $j => $el) {
                        $this->_row_data[$j + $this->_total_row][$f] = $el;
                    }
                }
            }
        }
        $this->_total_row = count($this->_row_data);
        return null;
    }

    private function readHeader(): array
    {
        $info = [];
        if ($this->gtV(self::DBMS_MIN_V_BLOCK_INFO)) {
            $info = [
                'num1'         => $this->read->number(),
                'is_overflows' => $this->read->number(),
                'num2'         => $this->read->number(),
                'bucket_num'   => $this->read->int(),
                'num3'         => $this->read->number()
            ];
        }
        $info['col_count']    = $this->read->number();
        $info['row_count']    = $this->read->number();

        return [$info['row_count'], $info['col_count']];
    }

    private function setServerInfo(): void
    {
        $this->_server_info              = [
            'name'          => $this->read->string(),
            'major_version' => $this->read->number(),
            'minor_version' => $this->read->number(),
            'version'       => $this->read->number(),
        ];
        $this->_server_info['time_zone'] = $this->gtV(self::DBMS_MIN_V_SERVER_TIMEZONE) ? $this->read->string() : '';
        $this->_server_info['display_name'] = $this->gtV(self::DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME) ? $this->read->string() : '';
        $this->_server_info['version_patch'] = $this->gtV(self::DBMS_MIN_REVISION_WITH_VERSION_PATCH) ? $this->read->number() : '';
    }


    private function sendQuery(string $sql, array $settings = []): void
    {
        $this->write->number(Protocol::CLIENT_QUERY, 0);

        if ($this->gtV(self::DBMS_MIN_V_CLIENT_INFO)) {

            // query kind, user, id, ip
            $this->write->number(1)->string('', '', '[::ffff:127.0.0.1]:0');
            if ($this->gtV(self::DBMS_MIN_REVISION_WITH_INITIAL_QUERY_START_TIME)) {
                $this->write->int64(0);
            }
            // iface type tcp, os ser, hostname
            $this->write->number(1)->string('', '');

            $this->addClientInfo();

            if ($this->gtV(self::DBMS_MIN_V_QUOTA_KEY_IN_CLIENT_INFO)) {
                $this->write->string('');
            }
            if ($this->gtV(self::DBMS_MIN_REVISION_WITH_DISTRIBUTED_DEPTH)) {
                $this->write->number(0);
            }
            if ($this->gtV(self::DBMS_MIN_REVISION_WITH_VERSION_PATCH)) {
                $this->write->number(0);
            }
            if ($this->gtV(self::DBMS_MIN_REVISION_WITH_OPENTELEMETRY)) {
                $this->write->number(0);
            }
        }

        if ($this->gtV(self::DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS)) {
            foreach ($settings as $name => $value) {
                $this->write->string($name)->number(1)->string($value);
            }
        }

        $this->write->number(0);

        if ($this->gtV(self::DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET)) {
            $this->write->number(0);
        }
        $this->write->number(Protocol::STAGES_COMPLETE, Protocol::COMPRESSION_DISABLE)->string($sql);
    }

    /**
     * @param string $sql
     */
    public function query(string $sql): bool|array
    {
        $this->fields = [];
        $this->sendQuery($sql);
        return $this->writeEnd();
    }

    /**
     * @param string $table
     * @param string[][] $data
     * @return array|bool
     * @throws CkException
     */
    public function insert(string $table, array $data): bool|array
    {
        $this->writeStart($table, array_keys($data[0]));
        $this->writeBlock($data);
        return $this->writeEnd();
    }

    /**
     * @param string $table
     * @param string[] $fields
     * @throws CkException
     */
    public function writeStart(string $table, array $fields): void
    {
        $this->fields = [];
        $table = trim($table);
        $this->sendQuery('INSERT INTO ' . $table . ' (' . implode(',', $fields) . ') VALUES ');
        $this->writeEnd(false);
        while (true) {
            $code = $this->read->number();
            if ($code == Protocol::SERVER_DATA) {
                $this->readData($code);
                break;
            } else if ($code == Protocol::SERVER_PROGRESS) {
                $this->readData($code);
                continue;
            } else if ($code == Protocol::SERVER_EXCEPTION) {
                $this->readErr();
            }
        }
    }

    /**
     * @param string[][] $data
     * @throws CkException
     */
    public function writeBlock(array $data): void
    {
        if (count($this->fields) === 0) {
            throw new CkException('Please execute first writeStart', CkException::CODE_TODO_WRITE_START);
        }
        $this->writeBlockHead();

        // column count , row Count
        $row_count = count($data);
        $this->write->number(count($data[0]), $row_count);

        $new_data = [];
        foreach ($data as $row) {
            foreach ($row as $k => $v) {
                $new_data[$k][] = $v;
            }
        }

        foreach ($new_data as $field => $data) {
            $type = $this->fields[$field];
            $this->write->string($field, $type);
            $this->types->pack($data, $type);
            $this->write->flush();
        }
        $this->write->flush();
    }

    /**
     * @param false $get_ret
     * @return array|bool
     * @throws CkException
     */
    public function writeEnd(bool $get_ret = true): bool|array
    {
        $this->writeBlockHead();
        $this->write->number(0);
        $this->write->number(0);
        $this->write->flush();
        if ($get_ret === true) {
            return $this->receive();
        }
        return true;
    }

    private function writeBlockHead(): void
    {
        $this->write->number(Protocol::CLIENT_DATA);
        if ($this->gtV(self::DBMS_MIN_V_TEMPORARY_TABLES)) {
            $this->write->number(0);
        }
        if ($this->gtV(self::DBMS_MIN_V_BLOCK_INFO)) {
            $this->write->number(1, 0, 2);
            $this->write->int(-1);
            $this->write->number(0);
        }
    }

    private function readErr(): void
    {
        $c   = $this->read->int();
        $n   = $this->read->string();
        $msg = $this->read->string();
        throw new CkException(substr($msg, strlen($n) + 1), $c);
    }
}
