<?php
/**
 * This file is part of ninja-mutex.
 *
 * (C) Eion Robb <eion@opmetrix.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
namespace NinjaMutex\Lock;

/**
 * Lock implementor using Couchbase
 *
 * @author Eion Robb <eion@opmetrix.com>
 */
class CouchbaseLock extends LockAbstract implements LockExpirationInterface
{
    /**
     * Maximum expiration time in seconds (30 days)
     */
    const MAX_EXPIRATION = 2592000;

    /**
    * The Couchbase collection object
     *
     * @var Couchbase\Collection
    */
    protected $collection = null;

    /**
    * The prefix to be used in Couchbase keynames.
    */
    protected $keyPrefix = 'lock:';

    /**
     * @var int Expiration time of the lock in seconds
     */
    protected $expiration = 0;

    /**
     * @param Couchbase\Collection $collection A couchbase collection created within a bucket, eg:
     *                                           $options = new \Couchbase\ClusterOptions();
     *                                           $options->credentials($username, $password);
     *                                           $connection = new \Couchbase\Cluster($host, $options);
     *                                           $bucket = $connection->bucket($bucketName);
     *                                           $bucket->setTranscoder('\Couchbase\passThruEncoder', '\Couchbase\passThruDecoder');
     *                                           $collection = $bucket->defaultCollection();
     *                                           $lock = new \NinjaMutex\Lock\CouchbaseLock($collection);
     */
    public function __construct($collection)
    {
        parent::__construct();

        $this->collection = $collection;
    }

    /**
     * @param int $expiration Expiration time of the lock in seconds. If it's equal to zero (default), the lock will never expire.
     *                        Max 2592000s (30 days), if greater it will be capped to 2592000 without throwing an error.
     *                        WARNING: Using value higher than 0 may lead to race conditions. If you set too low expiration time
     *                        e.g. 30s and critical section will run for 31s another process will gain lock at the same time,
     *                        leading to unpredicted behaviour. Use with caution.
     */
    public function setExpiration($expiration)
    {
        if ($expiration > static::MAX_EXPIRATION) {
            $expiration = static::MAX_EXPIRATION;
        }
        $this->expiration = $expiration;
    }

    /**
     * Clear lock without releasing it
     * Do not use this method unless you know what you do
     *
     * @param  string $name name of lock
     * @return bool
     */
    public function clearLock($name)
    {
        return true;
    }

    /**
     * @param  string $name name of lock
     * @param  bool   $blocking
     * @return bool
     */
    protected function getLock($name, $blocking)
    {
        $key = $this->keyPrefix . $name;

        $opts = new \Couchbase\UpsertOptions();
        $opts->expiry($this->expiration);

        $result = $this->collection->upsert($key, serialize($this->getLockInformation()), $opts);

        return $result ? true : false;
    }

    /**
     * Release lock
     *
     * @param  string $name name of lock
     * @return bool
     */
    public function releaseLock($name)
    {
        $key = $this->keyPrefix . $name;

        try {
            $result = $this->collection->remove($key);
        } catch (\Couchbase\DocumentNotFoundException $e) {
            $result = true;
        }
        
        // Release the lock in the parent (abstract) class.
        if ($result) unset($this->locks[$name]);

        return $result ? true : false;
    }

    /**
     * Check if lock is locked
     *
     * @param  string $name name of lock
     * @return bool
     */
    public function isLocked($name)
    {
        $key = $this->keyPrefix . $name;

        try {
            $res = $this->collection->get($key);
            $result = $res->content();

        } catch (\Couchbase\DocumentNotFoundException $e) {
            return false;
        }

        return $result !== false;
    }
}

