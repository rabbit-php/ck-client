<?php

namespace OneCk;

class Read
{

    public function __construct(private $conn)
    {
    }

    public function getChar(int $n = 1): string
    {
        $buffer = fread($this->conn, $n);
        if ($buffer === false || !isset($buffer[0])) {
            throw new CkException('read from fail', CkException::CODE_READ_FAIL);
        }
        if (strlen($buffer) < $n) {
            $buffer .= $this->getChar($n - strlen($buffer));
        }
        return $buffer;
    }

    /**
     * @return int
     * @throws CkException
     */
    public function number(): int
    {
        $r = 0;
        $b = 0;
        while (1) {
            $j = ord($this->getChar());
            $r = (($j & 127) << ($b * 7)) | $r;
            if ($j < 128) {
                return $r;
            }
            $b++;
        }
    }

    /**
     * @return int
     * @throws CkException
     */
    public function int(): int
    {
        return unpack('l', $this->getChar(4))[1];
    }

    /**
     * @return string
     * @throws CkException
     */
    public function string(): string
    {
        $n = $this->number();
        return $n === 0 ? '' : $this->getChar($n);
    }
}
