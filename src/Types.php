<?php

namespace OneCk;

class Types
{
    protected bool $is_null      = false;
    protected array $is_null_data = [];

    protected array $col_data = [];

    protected array $arr_dp   = [];
    protected string $arr_type = '';

    const BASE_TYPE = [
        'int8'    => ['c', 1],
        'uint8'   => ['C', 1],
        'int16'   => ['s', 2],
        'uint16'  => ['S', 2],
        'int32'   => ['l', 4],
        'uint32'  => ['L', 4],
        'int64'   => ['q', 8],
        // 'uint64'  => ['Q', 8],
        'float32' => ['f', 4],
        'float64' => ['d', 8]
    ];

    const ALIAS_TYPES = [
        'decimal32' => 'float32',
        'decimal64' => 'float64',
        'date'      => 'uint16',
        'datetime'  => 'uint32',
        'ipv4'      => 'uint32',
        'ipv6'      => 'fixedstring(16)',
        'enum8'     => 'int8',
        'enum16'    => 'int16',
        'nothing'   => 'int8'
    ];

    public function __construct(protected Write $write, protected Read $read)
    {
    }


    public static function encodeIpv4(string $ip): int
    {
        return ip2long($ip);
    }

    public static function encodeIpv6(string $ip): string
    {
        $ar = explode(':', $ip);
        if (count($ar) < 8 || strpos($ip, '::')) {
            $r = [];
            foreach ($ar as $v) {
                if (empty($v)) {
                    $r = array_merge($r, array_fill(0, 9 - count($ar), '0000'));
                    continue;
                }
                $r[] = str_pad($v, 4, '0', STR_PAD_LEFT);
            }
            $ar = $r;
        }
        return hex2bin(implode($ar));
    }

    public static function encodeFixedString(string $str, int $n): string
    {
        return str_pad($str, $n, chr(0));
    }

    public static function encodeDate(string $date): float
    {
        return ceil(strtotime($date) / 86400);
    }

    public static function encodeDatetime(string $datetime): int
    {
        return strtotime($datetime);
    }

    public static function encodeDatetime64(string $time, int $n = 3): int
    {
        $ar = explode('.', $time);
        $l  = isset($ar[1]) ? strlen($ar[1]) : 0;
        $n  = strtotime($ar[0]) . (isset($ar[1]) ? $ar[1] : '') . str_repeat('0', min(max($n - $l, 0), 9));
        return $n * 1;
    }

    public static function encodeUuid(string $data): string
    {
        $s  = str_replace('-', '', $data);
        $r1 = substr($s, 0, 8);
        $r2 = substr($s, 8, 8);
        $r3 = substr($s, 16, 8);
        $r4 = substr($s, 24);
        return pack('L4', hexdec($r2), hexdec($r1), hexdec($r4), hexdec($r3));
    }

    /**
     * @param string $n
     * @return string
     */
    public static function encodeInt128(string|int $n): string
    {
        if (!is_string($n)) {
            $n = "{$n}";
        }
        $is_n = false;
        if ($n[0] === '-') {
            $is_n = true;
        }
        $r = '';
        for ($i = 0; $i < 16; $i++) {
            $b = bcpow(2, 8);
            $c = bcmod($n, $b);
            $n = bcdiv($n, $b, 0);
            if ($is_n) {
                $v = ~abs(intval($c));
                if ($i === 0) {
                    $v = $v + 1;
                }
            } else {
                $v = intval($c);
            }
            $r .= chr($v);
        }
        return $r;
    }

    protected function encodeUint64(string|int $str): string
    {
        $str = "{$str}";
        return pack('L2', intval(bcmod($str, '4294967296')), intval(bcdiv($str, '4294967296')));
    }


    protected function decodeUint64(): string
    {
        $str = $this->read->getChar(8);
        $r   = unpack('L*', $str);
        return bcadd(bcmul($r[2], bcpow(2, 32)), $r[1]);
    }


    protected function decodeUuid(): string
    {
        $s = bin2hex($this->read->getChar(8));
        $r = '';
        for ($i = 14; $i >= 0; $i -= 2) {
            $r .= $s[$i] . $s[$i + 1];
        }
        $r = substr($r, 0, 8) . '-' . substr($r, 8, 4) . '-' . substr($r, 12);

        $s  = bin2hex($this->read->getChar(8));
        $r1 = '';
        for ($i = 14; $i >= 0; $i -= 2) {
            $r1 .= $s[$i] . $s[$i + 1];
        }
        $r .= '-' . substr($r1, 0, 4) . '-' . substr($r1, 4);
        return $r;
    }


    /**
     * @return string
     */
    protected function decodeInt128(): string
    {
        $str  = $this->read->getChar(16);
        $is_n = false;
        if (ord($str[15]) > 127) {
            $is_n = true;
        }
        $r = '0';
        for ($i = 0; $i < 16; $i++) {
            $n = ord($str[$i]);
            if ($is_n) {
                if ($i === 0) {
                    $n = $n - 1;
                }
                $n = ~$n & 255;
            }
            if ($n !== 0) {
                $r = bcadd(bcmul("{$n}", bcpow(2, 8 * $i)), $r);
            }
        }
        return $is_n ? '-' . $r : $r;
    }

    protected function decodeIpv6(string $data): string
    {
        $s = bin2hex($data);
        $r = [];
        $a = '';
        for ($i = 0; $i < 32; $i++) {
            $a .= $s[$i];
            if ($i < 31 && $i % 4 === 3) {
                $r[] = ltrim($a, '0');
                $a   = '';
            }
        }
        $r[] = ltrim($a, '0');
        $r   = implode(':', $r);
        while (strpos($r, ':::') !== false) {
            $r = str_replace(':::', '::', $r);
        }
        return $r;
    }


    public static function isDecimal(string $str): bool
    {
        return strpos($str, 'decimal(') === 0;
    }

    public static function isDatetime64(string $str): bool
    {
        return strpos($str, 'datetime64(') === 0;
    }


    public static function isArray(string $str): bool
    {
        return strpos($str, 'array(') === 0;
    }


    public static function isNullable(string $str): bool
    {
        return strpos($str, 'nullable(') === 0;
    }

    public static function isFixedString(string $str): bool
    {
        return strpos($str, 'fixedstring(') === 0;
    }

    public static function isSimpleAggregateFunction(string $str): bool
    {
        return strpos($str, 'simpleaggregatefunction(') === 0;
    }

    public static function isEnum(string $str): bool
    {
        return strpos($str, 'enum') !== false;
    }

    public static function isLowcardinality(string $str): bool
    {
        return strpos($str, 'lowcardinality') !== false;
    }

    protected function alias(string &$tp): string
    {
        $type = $tp;
        if (isset(self::BASE_TYPE[$type]) || $type === 'string' || self::isFixedString($type)) {
            return $type;
        }
        if (isset(self::ALIAS_TYPES[$type])) {
            return self::ALIAS_TYPES[$type];
        }
        if (self::isNullable($type)) {
            $this->is_null = true;
            $tp            = substr($type, 9, -1);
            return $this->alias($tp);
        }
        if (self::isDecimal($type)) {
            $arr = explode(',', substr($type, 8, -1));
            if ($arr[0] < 10) {
                return 'int32';
            } else if ($arr[0] < 19) {
                return 'int64';
            } else {
                return 'int128';
            }
        }
        if (self::isDatetime64($type)) {
            return 'uint64';
        }
        if (self::isSimpleAggregateFunction($type)) {
            $tp   = substr(trim(strstr($type, ','), ' ,'), 0, -1);
            $type = $tp;
            return $this->alias($type);
        }
        if (self::isEnum($type)) {
            $pos = strpos($type, '(');
            $tp = substr($type, 0, $pos === false ? null : $pos);
            $type = $tp;
            return $this->alias($type);
        }
        if (self::isLowcardinality($type)) {
            $tp = substr($type, strpos($type, '(') + 1, strpos($type, ')') - strpos($type, '(') - 1);
            $type = $tp;
            return $this->alias($type);
        }
        $is_arr = false;
        while (self::isArray($type)) {
            $this->arr_dp[] = 'array';
            $type           = substr($type, 6, -1);
            $is_arr         = true;
        }
        if ($is_arr) {
            $this->arr_type = $type;
            return $this->alias($this->arr_type);
        }
        return $type;
    }


    protected function getArrData(int $row_count, string $real_type): array
    {
        $deep  = count($this->arr_dp);
        $data  = array_fill(0, $row_count, []);
        $arr   = [];
        $els   = [];
        $first = true;
        while ($deep--) {
            $del = [];
            $l   = count($data);
            $p   = 0;
            foreach ($data as $i => &$val) {
                if ($first) {
                    $arr[] = &$val;
                }
                $num = unpack('Q', $this->read->getChar(8))[1]; //array len is uint64 ->  PHP_INT_MAX
                $val = array_fill(0, $num - $p, []);
                $p   = $num;
                foreach ($val as &$v) {
                    if ($deep > 0) {
                        $data[] = &$v;
                    } else {
                        $els[] = &$v;
                    }
                }
                $del[] = $i;
                $l--;
                if ($l === 0) {
                    break;
                }
            }
            foreach ($del as $i) {
                unset($data[$i]);
            }
            $first = false;
        }

        $row_count = count($els);
        $this->getNull($row_count);
        $this->decode($real_type, $row_count);
        foreach ($this->is_null_data as $i => $v) {
            $this->col_data[$i] = null;
        }
        $this->unFormat($this->arr_type);
        foreach ($els as $i => &$v) {
            $v = $this->col_data[$i];
        }
        $this->col_data = [];
        $this->arr_dp   = [];

        return isset($els[0]) ? $arr : [];
    }

    protected function setArrData(mixed $in_da, string $type, string $real_type): void
    {
        $data   = [];
        $index  = [$in_da];
        $r      = [];
        $arr_dp = 0;
        while (true) {
            $del = [];
            $j   = 0;
            foreach ($index as $i => $val) {
                $j     += count($val);
                $r[]   = $j;
                $del[] = $i;
                if (isset($val[0]) && is_array($val[0])) {
                    foreach ($val as $v) {
                        $index[] = $v;
                    }
                } else {
                    $data = array_merge($data, $val);
                }
            }
            foreach ($del as $i) {
                unset($index[$i]);
            }
            if (empty($index)) {
                break;
            }
            $arr_dp++;
        }

        if (count($this->arr_dp) !== $arr_dp) {
            throw new CkException('array deep err', CkException::CODE_ARR_ERR);
        }
        array_shift($r);
        $this->write->addBuf(pack('Q' . '*', ...$r)); // array len is uint64 ->  PHP_INT_MAX
        $this->setNull($data);
        $this->format($data, $this->arr_type);
        $this->encode($data, $type, $real_type);

        $this->arr_dp = [];
    }


    protected function getNull(int $row_count): void
    {
        $this->is_null_data = [];
        if ($this->is_null) {
            for ($i = 0; $i < $row_count; $i++) {
                $n = $this->read->number();
                if ($n === 1) {
                    $this->is_null_data[$i] = 1;
                }
            }
        }
    }

    /**
     * @param $data
     */
    protected function setNull(array &$data): void
    {
        if ($this->is_null) {
            foreach ($data as $i => &$v) {
                if ($v === null) {
                    $this->is_null_data[$i] = 1;
                    $v                      = 0;
                } else {
                    $this->is_null_data[$i] = 0;
                }
            }
            $this->write->addBuf(pack('C*', ...$this->is_null_data));
        }
        $this->is_null_data = [];
    }


    /**
     * @param string[] $data
     * @param $type
     * @return mixed|null
     */
    protected function format(array &$data, string $type): int
    {
        if (isset(self::BASE_TYPE[$type]) || $type === 'string') {
            return 1;
        }
        $call = [
            'date'     => function ($v) {
                return self::encodeDate($v);
            },
            'datetime' => function ($v) {
                return self::encodeDatetime($v);
            },
            'ipv4'     => function ($v) {
                return self::encodeIpv4($v);
            },
            'ipv6'     => function ($v) {
                return self::encodeIpv6($v);
            }
        ];
        $fn   = null;
        if (isset($call[$type])) {
            $fn = $call[$type];
        } else if (self::isDecimal($type)) {
            $arr = explode(',', substr($type, 8, -1));
            if ($arr[0] >= 19) {
                $fn = function ($v) use ($arr) {
                    $tr = explode('.', "{$v}");
                    isset($tr[1]) || $tr[1] = '';
                    return $tr[0] . $tr[1] . str_repeat('0', max(0, $arr[1] - strlen($tr[1])));
                };
            } else {
                $fn = function ($v) use ($arr) {
                    return $v * pow(10, $arr[1]);
                };
            }
        } else if (self::isDatetime64($type)) {
            $n  = substr($type, 11, -1);
            $fn = function ($v) use ($n) {
                return self::encodeDatetime64($v, $n);
            };
        }

        if ($fn) {
            foreach ($data as &$el) {
                if ($el !== null) {
                    $el = $fn($el);
                }
            }
        }
        return 1;
    }


    protected function unFormat(string $type): int
    {
        if (isset(self::BASE_TYPE[$type]) || $type === 'string' || $type === 'uuid' || self::isFixedString($type) || $type === 'nothing') {
            return 1;
        }

        $call = [
            'date'     => function ($v) {
                return date('Y-m-d', $v * 86400);
            },
            'datetime' => function ($v) {
                return date('Y-m-d H:i:s', $v);
            },
            'ipv4'     => function ($v) {
                return long2ip($v);
            },
            'ipv6'     => function ($v) {
                return $this->decodeIpv6($v);
            }
        ];

        $fn = null;
        if (isset($call[$type])) {
            $fn = $call[$type];
        } else if (self::isDecimal($type)) {
            $arr = explode(',', substr($type, 8, -1));
            if ($arr[0] >= 19) {
                $fn = function ($v) use ($arr) {
                    $v = "{$v}";
                    return substr($v, 0, -$arr[1]) . '.' . substr($v, -$arr[1]);
                };
            } else {
                $fn = function ($v) use ($arr) {
                    return $v / pow(10, $arr[1]);
                };
            }
        } else if (self::isDatetime64($type)) {
            $fn = function ($v) {
                return date('Y-m-d H:i:s', substr($v, 0, 10)) . '.' . substr($v, 10);
            };
        }

        if ($fn === null) {
            return 1;
        }

        foreach ($this->col_data as &$el) {
            if ($el !== null) {
                $el = $fn($el);
            }
        }
        return 1;
    }


    /**
     * @param $type
     * @return mixed|string
     * @throws CkException
     */
    protected function decode(string $type, int $row_count): int
    {
        if ($row_count === 0) {
            return 1;
        }

        if (isset(self::BASE_TYPE[$type])) {
            $this->col_data = array_values(
                unpack(
                    self::BASE_TYPE[$type][0] . '*',
                    $this->read->getChar(self::BASE_TYPE[$type][1] * $row_count)
                )
            );
            return 1;
        }
        $fn = null;
        if ($type === 'string') {
            $fn = function () {
                return $this->read->string();
            };
        } else if ($type === 'uint64') {
            $fn = function () {
                return $this->decodeUint64();
            };
        } else if (self::isFixedString($type)) {
            $n  = intval(substr($type, 12, -1));
            $fn = function () use ($n) {
                return $this->read->getChar($n);
            };
        } else if ($type === 'int128') {
            $fn = function () {
                return $this->decodeInt128();
            };
        } else if ($type === 'uuid') {
            $fn = function () {
                return $this->decodeUuid();
            };
        } else {
            throw new CkException('not supported type :' . $type, CkException::CODE_NOT_SUPPORTED_TYPE);
        }
        $this->col_data = [];
        for ($i = 0; $i < $row_count; $i++) {
            $this->col_data[] = $fn();
        }
        return 1;
    }

    /**
     * @param string[] $data
     * @param string $type
     * @param string $real_type
     * @throws CkException
     */
    protected function encode(array $data, string $type, string $real_type): int
    {
        if (isset(self::BASE_TYPE[$real_type])) {
            $this->write->addBuf(pack(self::BASE_TYPE[$real_type][0] . '*', ...$data));
            return 1;
        }
        $fn = null;
        if ($real_type === 'string') {
            $fn = function ($v) {
                $this->write->string($v);
            };
        } else if ($real_type === 'uint64') {
            $fn = function ($v) {
                $this->write->addBuf($this->encodeUint64($v));
            };
        } else if (self::isFixedString($real_type)) {
            $n  = intval(substr($real_type, 12, -1));
            $fn = function ($v) use ($n) {
                $this->write->addBuf(self::encodeFixedString($v, $n));
            };
        } else if ($real_type === 'int128') {
            $fn = function ($v) {
                $this->write->addBuf(self::encodeInt128($v));
            };
        } else if ($real_type === 'uuid') {
            $fn = function ($v) {
                $this->write->addBuf(self::encodeUuid($v));
            };
        } else {
            throw new CkException('unset type :' . $type, CkException::CODE_UNSET_TYPE);
        }
        foreach ($data as $el) {
            $fn($el);
        }
        return 1;
    }


    public function unpack(string $type, int $row_count): array
    {
        $type          = strtolower($type);
        $this->is_null = false;
        $real_type     = $this->alias($type);
        if (isset($this->arr_dp[0])) {
            return $this->getArrData($row_count, $real_type);
        } else {
            $this->getNull($row_count);
            $this->decode($real_type, $row_count);
            foreach ($this->is_null_data as $i => $v) {
                $this->col_data[$i] = null;
            }
            $this->unFormat($type);
            return $this->col_data;
        }
    }

    public function pack(array $data, string $type): void
    {
        $type          = strtolower($type);
        $this->is_null = false;
        $real_type     = $this->alias($type);
        $this->format($data, $type);
        if (isset($this->arr_dp[0])) {
            $this->setArrData($data, $type, $real_type);
        } else {
            $this->setNull($data);
            $this->encode($data, $type, $real_type);
        }
    }
}
