<?php
/**
 * Created by JetBrains PhpStorm.
 * User: jamesmuir
 * Date: 1/5/12
 * Time: 11:40 AM
 * To change this template use File | Settings | File Templates.
 */
class ParseCsv implements Iterator
{

    protected $files = array();
    protected $position = 0;

    private $logsDir;

    public function __construct($logsDir)
    {
        $this->logsDir = $logsDir;

        $this->initFiles();
    }

    protected  function initFiles()
    {



        if ($handle = opendir($this->logsDir)) {
            while (false !== ($entry = readdir($handle))) {
                if ($entry != "." && $entry != "..") {
                    //echo "$entry\n";
                    $this->files[] = $this->logsDir . $entry;
                }
            }
            closedir($handle);
        }
    }


    protected  function parse_file($file)
    {

        $row = 0;
        $headers = array();
        $json = array();
        if (($handle = fopen($file, "r")) !== FALSE) {
            while (($data = fgetcsv($handle, 1000, ",")) !== FALSE) {
                $row++;
                if ($row == 1) {

                    $headers = $data;
                    continue;
                }
                $tmp = array();
                $num = count($data);
                for ($c = 0; $c < $num; $c++) {
                    $tmp[$headers[$c]] = $data[$c];
                }
                $json[] = $tmp;

            }
            fclose($handle);
        }


        return $json;
    }

    /**
     * (PHP 5 &gt;= 5.1.0)<br/>
     * Return the current element
     * @link http://php.net/manual/en/iterator.current.php
     * @return mixed Can return any type.
     */
    public function current()
    {
        return $this->parse_file($this->files[$this->position]);
    }

    /**
     * (PHP 5 &gt;= 5.1.0)<br/>
     * Move forward to next element
     * @link http://php.net/manual/en/iterator.next.php
     * @return void Any returned value is ignored.
     */
    public function next()
    {
        $this->position++;
    }

    /**
     * (PHP 5 &gt;= 5.1.0)<br/>
     * Return the key of the current element
     * @link http://php.net/manual/en/iterator.key.php
     * @return scalar scalar on success, integer
     * 0 on failure.
     */
    public function key()
    {
        $this->position;
    }

    /**
     * (PHP 5 &gt;= 5.1.0)<br/>
     * Checks if current position is valid
     * @link http://php.net/manual/en/iterator.valid.php
     * @return boolean The return value will be casted to boolean and then evaluated.
     * Returns true on success or false on failure.
     */
    public function valid()
    {
        return isset($this->files[$this->position]);
    }

    /**
     * (PHP 5 &gt;= 5.1.0)<br/>
     * Rewind the Iterator to the first element
     * @link http://php.net/manual/en/iterator.rewind.php
     * @return void Any returned value is ignored.
     */
    public function rewind()
    {
        $this->position = 0;
    }
}
