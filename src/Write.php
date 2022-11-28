<?php

namespace OneCk;

class Write
{
    private string $buf = '';

    public function __construct(private $conn)
    {
    }


    /**
     * @param int ...$nr
     * @return $this
     */
    public function number(...$nr): self
    {
        $r = [];
        foreach ($nr as $n) {
            $b = 0;
            while ($n >= 128) {
                $r[] = $n | 128;
                $b++;
                $n = $n >> 7;
            }
            $r[] = $n;
        }
        if ($r) {
            $this->buf .= pack('C*', ...$r);
        }
        return $this;
    }

    /**
     * @param int $n
     * @return $this
     */
    public function int(int $n): self
    {
        $this->buf .= pack('l', $n);
        return $this;
    }

    public function int64(int $i): self
    {
        $this->buf .= pack("q", $i);
        return $this;
    }

    /**
     * @param int ...$str
     * @return $this
     */
    public function string(...$str): self
    {
        foreach ($str as $s) {
            $this->number(strlen($s));
            $this->buf .= $s;
        }
        return $this;
    }


    /**
     * @param $str
     * @return $this
     */
    public function addBuf(string $str): self
    {
        $this->buf .= $str;
        return $this;
    }


    public function flush(): bool
    {
        if ($this->buf === '') {
            return true;
        }
        $len = fwrite($this->conn, $this->buf);
        if ($len !== strlen($this->buf)) {
            throw new CkException('write fail', CkException::CODE_WRITE_FAIL);
        }
        $this->buf = '';
        return true;
    }
}
