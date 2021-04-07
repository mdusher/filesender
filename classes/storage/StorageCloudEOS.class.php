<?php
/*
 * Store the file as chunks instead of as a single file on disk.
 *
 * FileSender www.filesender.org
 *
 * Copyright (c) 2009-2012, AARNet, Belnet, HEAnet, SURFnet, UNINETT
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *     Neither the name of AARNet, Belnet, HEAnet, SURFnet and UNINETT nor the
 *     names of its contributors may be used to endorse or promote products
 *     derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

if (!defined('FILESENDER_BASE')) {
    die('Missing environment');
}

/**
 * Generic exception so we can throw a specific exception with a message on failed writes
 */
class EOSWriteException extends \Exception {}

/**
 * Storage class for use with EOS
 * 
 * This class uses xrdcp and xrdfs to interact with the storage so will 
 * require xrootd-client (only tested with RHEL7 and CentOS7)
 *
 */

class StorageCloudEOS extends StorageFilesystem {
    // Time to wait between retries
    protected static $sleepRetry = 400000; //400ms
    // Maximum number of times to retry anything
    protected static $maxRetry = 10;
    // Maximum number of times to retry writes
    protected static $maxWriteRetry = 20;

    /**
     * getOffsetWithinChunkedFile()
     *
     * @param string $filePath
     * @param int $offset
     * @return int
     */
    public static function getOffsetWithinChunkedFile($filePath, $offset)
    {
        $file_chunk_size = Config::get('upload_chunk_size');
        return ($offset % $file_chunk_size);
    }

    /**
     * Generate the filename for the chunk
     *
     * @param string $filePath
     * @param int $offset
     * @return string
     */
    public static function getChunkFilename($filePath, $offset)
    {
        $file_chunk_size = Config::get('upload_chunk_size');
        $offset = $offset - ($offset % $file_chunk_size);
        return $filePath.'/'.str_pad($offset, 24, '0', STR_PAD_LEFT);
    }

    /**
     *  Reads chunk at offset
     *
     * @param File $file
     * @param uint $offset offset in bytes
     * @param uint $length length in bytes
     *
     * @return mixed chunk data encoded as string or null if no chunk remaining
     *
     * @throws StorageFilesystemFileNotFoundException
     * @throws StorageFilesystemCannotReadException
     */
    public static function readChunk(File $file, $offset, $length)
    {
        if ($file->transfer->options['encryption']) {
            $offset=$offset/Config::get('upload_chunk_size')*Config::get('upload_crypted_chunk_size');
        }

        $filePath = self::buildPath($file);
        $chunkFile = self::getChunkFilename($filePath, $offset);

        if (!self::file_exists($chunkFile)) {
            Logger::error('readChunk() failed: '.$chunkFile.' does not exist.');
            throw new StorageFilesystemFileNotFoundException($chunkFile, $file);
        }

        $data = self::file_get_contents($chunkFile);
        if ($data === false) {
            Logger::error('readChunk() failed: '.$chunkFile.' failed to read file.');
            throw new StorageFilesystemCannotReadException($filePath, $file);
        }
        return $data;
    }

    /**
     * Return the path to the directory that holds the file chunks
     * @param File $file
     * @return string
     */
    public static function buildPath(File $file)
    {
        return parent::buildPath($file) . $file->uid;
    }

    /**
     * Write a chunk of data to file at offset
     *
     * @param File $file
     * @param string $data the chunk data
     * @param uint $offset offset in bytes
     *
     * @return array with offset and written amount of bytes
     *
     * @throws StorageFilesystemOutOfSpaceException
     * @throws StorageFilesystemCannotWriteException
     */
    public static function writeChunk(File $file, $data, $offset = null)
    {
        $chunkSize = \strlen($data);
        $filePath = self::buildPath($file);

        if (self::file_exists($filePath) === false) {
            self::mkdir($filePath);
        }

        $chunkFile = self::getChunkFilename($filePath, $offset);
        $validUpload = false;
        for ($attempt = 1; $attempt <= self::$maxWriteRetry; $attempt++) {
            $isLastAttempt = $attempt >= self::$maxWriteRetry;
            try {
                $written = self::file_put_contents($chunkFile, $data);

                // Check that the right amount of data was written and move to next iteration if it fails
                if ($chunkSize != $written) {
                    throw new EOSWriteException('chunk_size != bytes written');
                }

                // Clear cached values and check the chunk file now exists, otherwise we retry from the beginning
                if (!self::file_exists($chunkFile)) {
                    throw new EOSWriteException('file does not exist after write');
                }

                // Check file size
                if ($chunkSize != self::filesize($chunkFile)) {
                    throw new EOSWriteException('chunk_size != filesize()');
                }

                // Hash the file to make sure it actually exists and matches the written data
                if (!self::verifyChecksum('md5', $data, $chunkFile)) {
                    throw new EOSWriteException('checksum validation failed');
                }

                $validUpload = true;
                // Passed all the validation so I guess let's get out of this loop
                break;
            } catch (EOSWriteException $e) {
                if ($isLastAttempt) {
                    Logger::error("writeChunk() failed: {$chunkFile} {$e->getMessage()}");
                    // Re-throw any StorageFilesystemCannotWriteException so that we can add extra information
                    throw new StorageFilesystemCannotWriteException($filePath, $file, $data, $offset, $written);
                } else {
                    Logger::debug("writeChunk() failed: {$chunkFile} {$e->getMessage()}");
                }
            }
            usleep(self::$sleepRetry);
        }

        // Just in case we get through all of our attempts and don't hit the try/catch
        if ($validUpload === false && $isLastAttempt) {
            Logger::error("writeChunk() failed: {$chunkFile} exceeded retry attempts");
            throw new StorageFilesystemCannotWriteException($filePath, $file, $data, $offset, $written);
        }

        // Don't reach the end if the chunk is invalid.
        if ($validUpload === false) {
            Logger::error("writeChunk() failed: {$chunkFile} was an invalid upload");
            throw new StorageFilesystemCannotWriteException($filePath, $file, $data, $offset, $written);
        }

        return array(
            'offset' => $offset,
            'written' => $written
        );
    }

    /**
     * Helper method to validate the integrity of a file vs the incoming stream
     * @param string $algo
     * @param string $stream
     * @param string $path
     * @return bool
     */
    public static function verifyChecksum($algo, $stream, $path)
    {
        $fileHash = self::hash_file($algo, $path);
        if ($fileHash === false) {
            Logger::error("verifyChecksum() unable to hash file: $path");
            return false;
        }

        $streamHash = \hash($algo, $stream);
        if ($fileHash === $streamHash) {
            return true;
        }

        Logger::error("verifyChecksum() checksums do not match. file: $path file_hash: $fileHash stream_hash: $streamHash");
        return false;
    }

    /**
     * Handles file completion checks
     *
     * @param File $file
     */
    public static function completeFile(File $file)
    {
        self::setup();
        $filePath = self::buildPath($file);
        $expectedSize = $file->size;
        $onDiskSize = 0;

        for ($attempt = 1; $attempt <= self::$maxRetry; $attempt++) {
            $isLastAttempt = $attempt >= self::$maxRetry;
            // Use the size on the directory
            $onDiskSize = self::filesize($filePath);

            if ($file->transfer->is_encrypted) {
                $expectedSize = $file->encrypted_size;
            }

            if ($onDiskSize == $expectedSize) {
                return;
            }
            usleep(self::$sleepRetry);
        }

        Logger::error('completeFile('.$file->uid.') size mismatch. expected_size:'.$expectedSize.' ondisk_size:'.$onDiskSize);
        throw new FileIntegrityCheckFailedException($file, 'Expected size was '.$expectedSize.' but size on disk is '.$onDiskSize);
    }

    /**
     * Calculate the size of the uploaded file by summing all files in the directory
     * @param string $path
     * @return int
     */
    public static function calculateOnDiskSize($path)
    {
        $dirList = self::xrdfsLs($path);
        $size = 0;
        foreach ($dirList as $child) {
            if (trim($child) !== "") {
                $size += self::filesize($child);
            }
        }
        return $size;
    }

    /**
     * Deletes a file
     *
     * @param File $file
     *
     * @throws StorageFilesystemCannotDeleteException
     */
    public static function deleteFile(File $file)
    {
        $filePath = self::buildPath($file);
        $dirList = self::xrdfsLs($filePath);
        foreach ($dirList as $child) {
            self::xrdfsRm($child);
        }
        self::xrdfsRmdir($filePath);
    }

    /**
     * Store a whole file
     *
     * @param File $file
     * @param string $source_path path to file data
     *
     * @return bool
     *
     * @throws StorageFilesystemOutOfSpaceException
     */
    public static function storeWholeFile(File $file, $source_path)
    {
        return self::writeChunk($file, \file_get_contents($source_path), 0);
    }

    /**
     * Get a resource stream (used with the archiver to generate tar/zip files)
     *
     * @param File $file
     * @return resource
     */
    public static function getStream(File $file)
    {
        StorageFilesystemChunkedStream::ensureRegistered();
        $path = "StorageFilesystemChunkedStream://" . $file->uid;
        $fp = \fopen($path, "r+");
        return $fp;
    }

    /******************************************
     * helper methods for php native functions
     ******************************************/

     /**
     * Helper method for mkdir()
     * @param string $path
     * @param $data
     * @return int|false
     */
    public static function mkdir($path)
    {
        return self::xrdfsMkdir($path);
    }

    /**
     * Helper method for file_put_contents()
     * @param string $path
     * @param $data
     * @return int|false
     */
    public static function file_put_contents($path, $data)
    {
        $written = false;
        for ($attempt = 1; $attempt <= self::$maxWriteRetry; $attempt++) {
            $isLastAttempt = $attempt >= self::$maxWriteRetry;
            $written = self::xrdWrite($path, $data);
            if ($written !== false) {
                break;
            }
            if ($isLastAttempt) {
                Logger::error("file_put_contents({$path}): Failed to write file after " . self::$maxWriteRetry . " attempts");
                return false;
            }
            usleep(self::$sleepRetry);
        }
        return $written;
    }

    /**
     * Helper method for file_get_contents()
     * @param string $path
     * @return blob|false
     */
    public static function file_get_contents($path)
    {
        $data = false;
        for ($attempt = 1; $attempt <= self::$maxRetry; $attempt++) {
            $isLastAttempt = $attempt >= self::$maxRetry;
            $data = self::xrdRead($path);
            if ($data !== false) {
                break;
            }
            if ($isLastAttempt) {
                Logger::error("file_get_contents({$path}): Failed to read file after " . self::$maxRetry . " attempts");
                return false;
            }
            usleep(self::$sleepRetry);
        }
        return $data;
    }

    /**
     * Helper method for file_exists()
     * @param string $path
     * @return bool
     */
    public static function file_exists($path)
    {
        $stat = self::xrdfsStat($path);
        return ($stat !== false);
    }

    /**
     * Helper method for filesize()
     * @param string $path
     * @return int|false
     */
    public static function filesize($path)
    {
        $stat = self::xrdfsStat($path);
        if ($stat === false || !array_key_exists('size', $stat)) {
            return false;
        }
        return $stat['size'];
    }

    /**
     * Helper method for hash_file()
     * @param string $path
     * @return int|false
     */
    public static function hash_file($algo, $path)
    {
        $data = self::file_get_contents($path);
        $hash = \hash($algo, $data);
        return $hash;
    }

    /******************************************
     * xrdcp / xrdfs helper methods
     ******************************************/

    /**
     * Get the MGM URI from the config and make sure it has a trailing slash.
     * @return string
     */
    public static function xrdcpMgmUri()
    {
        $mgmUri = rtrim(Config::get("xrdcp_mgm_uri"), '/') . '/';
        return $mgmUri;
    }

    /**
     * Create a directory using xrdfs
     * @param string $path
     * @return string
     */
    public static function xrdfsExec($cmd)
    {
        $std = [ ["pipe", "r"], ["pipe", "w"], ["pipe", "w"] ];
        $process = proc_open($cmd, $std, $pipes);
        if (is_resource($process)) {
            \fclose($pipes[0]);

            $stdout = stream_get_contents($pipes[1]);
            $stderr = stream_get_contents($pipes[2]);
            $return_code = proc_close($process);
            return [$return_code, $stdout, $stderr];
        }

        return [false, null, 'failed to execute command.'];
    }

    /**
     * Create a directory using xrdfs
     * @param string $path
     * @return string
     */
    public static function xrdfsMkdir($path)
    {
        $cmd = sprintf('xrdfs %s mkdir -p %s', \escapeshellarg(self::xrdcpMgmUri()), \escapeshellarg($path));
        list($return_code, $stdout, $stderr) = self::xrdfsExec($cmd);

        if ($return_code === 0) {
            return true;
        } else {
            Logger::error("xrdfsMkdir({$path}): {$stderr} (return code: {$return_code})");
        }
        return false;
    }

    /**
     * List a directory using xrdfs
     * @param string $path
     * @return string
     */
    public static function xrdfsLs($path)
    {
        $cmd = sprintf('xrdfs %s ls %s', \escapeshellarg(self::xrdcpMgmUri()), \escapeshellarg($path));
        list($return_code, $stdout, $stderr) = self::xrdfsExec($cmd);
        if ($return_code === 0) {
            $list = explode("\n", $stdout);
            // Trim off the trailing \n's
            $list = array_map(function ($item) { return rtrim($item); }, $list);
            return $list;
        } else {
            Logger::error("xrdfsLs({$path}): {$stderr} (return code: {$return_code})");
        }
        return false;
    }

    /**
     * Remove a directory using xrdfs (directory must be empty)
     * @param string $path
     * @return string
     */
    public static function xrdfsRmdir($path)
    {
        $cmd = sprintf('xrdfs %s rmdir %s', \escapeshellarg(self::xrdcpMgmUri()), \escapeshellarg($path));
        list($return_code, $stdout, $stderr) = self::xrdfsExec($cmd);

        if ($return_code === 0) {
            return true;
        } else {
            Logger::error("xrdfsRmdir({$path}): {$stderr} (return code: {$return_code})");
        }
        return false;
    }

    /**
     * Remove a file using xrdfs
     * @param string $path
     * @return string
     */
    public static function xrdfsRm($path)
    {
        $cmd = sprintf('xrdfs %s rm %s', \escapeshellarg(self::xrdcpMgmUri()), \escapeshellarg($path));
        list($return_code, $stdout, $stderr) = self::xrdfsExec($cmd);
        if ($return_code === 0) {
            return true;
        } else {
            Logger::error("xrdfsRm({$path}): {$stderr} (return code: {$return_code})");
        }
        return false;
    }

    /**
     * Perform a file stat on the path provided using xrdfs
     * @param string $path
     * @return string
     */
    public static function xrdfsStat($path)
    {
        $cmd = sprintf('xrdfs %s stat %s', \escapeshellarg(self::xrdcpMgmUri()), \escapeshellarg($path));
        list($return_code, $stdout, $stderr) = self::xrdfsExec($cmd);

        if ($return_code === 0) {
            // Take the output from `xrdfs stat` and turn it into an array
            $statsplit = explode("\n", $stdout);
            $stat = Array();
            foreach ($statsplit as $value) {
                $keysplit = explode(":", $value);
                if (count($keysplit) > 1) {
                    $key = strtolower(array_shift($keysplit));
                    $statvalue = trim(implode(":", $keysplit));
                    $stat[$key] = $statvalue;
                }
            }
            // Only return if we got some values
            if (count($stat) > 0) {
                return $stat;
            }
        }

        // If we reach here, the file either doesn't exist or something went wrong.
        Logger::error("xrdfsStat({$path}): {$stderr} (return code: {$return_code})");
        return false;
    }

    /**
     * Write a file to EOS by passing data to xrdcp via stdin
     * @param string $path
     * @param string $data
     * @return int|false
     */
    public static function xrdWrite($path, $data)
    {
        if (!$path || !$data) {
            return false;
        }

        $cmd = sprintf('xrdcp -s -f - %s', \escapeshellarg(self::xrdcpMgmUri().$path));
        $std = [ ["pipe", "r"], ["pipe", "w"], ["pipe", "w"] ];
        $process = proc_open($cmd, $std, $pipes);

        if (is_resource($process)) {
            $result = true;
            $bytesWritten = \fwrite($pipes[0], $data);
            if ($bytesWritten === false || $bytesWritten != \strlen($data) ) {
                // write error, could be disk full ?
                $result = false;
            }
            \fclose($pipes[0]);

            $stderr = stream_get_contents($pipes[2]);
            $return_code = proc_close($process);

            if ($return_code === 0 && $result === true) {
                return $bytesWritten;
            }
            if ($return_code !== 0) {
                Logger::error("xrdWrite({$path}): {$stderr} (return code: {$return_code})");
            }
        }
        return false;
    }

    /**
     * Read a file from EOS using xrdcp and reading from stdout
     * @param string $path
     * @return int|false
     */
    public static function xrdRead($path)
    {
        $cmd = sprintf('xrdcp -s %s -', \escapeshellarg(self::xrdcpMgmUri().$path));
        $std = [ ["pipe", "r"], ["pipe", "w"], ["pipe", "w"] ];
        $process = proc_open($cmd, $std, $pipes);
        if (is_resource($process)) {
            \fwrite($pipes[0], $path);
            \fclose($pipes[0]);

            $result = Array();
            $result["stdout"] = stream_get_contents($pipes[1]);
            $result["stderr"] = stream_get_contents($pipes[2]);
            $return_code = proc_close($process);

            if ($return_code === 0) {
                return $result["stdout"];
            } else {
                Logger::error("xrdRead({$path}): {$result["stderr"]} (return code: {$return_code})");
            }
        }

        return false;
    }
}

