<?php

/*
 * FileSender www.filesender.org
 * 
 * Copyright (c) 2009-2014, AARNet, Belnet, HEAnet, SURFnet, UNINETT
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * *	Redistributions of source code must retain the above copyright
 * 	notice, this list of conditions and the following disclaimer.
 * *	Redistributions in binary form must reproduce the above copyright
 * 	notice, this list of conditions and the following disclaimer in the
 * 	documentation and/or other materials provided with the distribution.
 * *	Neither the name of AARNet, Belnet, HEAnet, SURFnet and UNINETT nor the
 * 	names of its contributors may be used to endorse or promote products
 * 	derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 'AS IS'
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

// Require environment (fatal)
if(!defined('FILESENDER_BASE')) die('Missing environment');

/**
 * Represents an user in database
 */
class StatLog extends DBObject {
    /**
     * Database map
     */
    protected static $dataMap = array(
        'id' => array(
            'type' => 'uint',
            'size' => 'medium',
            'primary' => true,
            'autoinc' => true
        ),
        'event' => array(
            'type' => 'string',
            'size' => 20,
        ),
        'target_type' => array(
            'type' => 'string',
            'size' => 255
        ),
        'size' => array(
            'type' => 'int',
            'size' => 'medium',
        ),
        
        'created' => array(
            'type' => 'datetime'
        )
    );
    
    /**
     * Properties
     */
    protected $id = null;
    protected $event = null;
    protected $created = null;
    protected $target_type = null;
    protected $size = null;
    
    
    /**
     * Constructor
     * 
     * @param integer $id identifier of user to load from database (null if loading not wanted)
     * @param array $data data to create the user from (if already fetched from database)
     * 
     * @throws UserNotFoundException
     */
    protected function __construct($id = null, $data = null) {
        if(!is_null($id)) {
            $statement = DBI::prepare('SELECT * FROM '.self::getDBTable().' WHERE id = :id');
            $statement->execute(array(':id' => $id));
            $data = $statement->fetch();
            if(!$data) throw new StatLogNotFoundException('id = '.$id);
        }
        
        if($data) $this->fillFromDBData($data);
    }
    
    /**
     * Save in database
     */
    public function save() {
        $this->insertRecord($this->toDBData());
    }
    
    /**
     * Create a new stat log
     * 
     * @param StatEvent $event: the event to be logged
     * @param DBObject: the target to be logged
     * 
     * @return StatLog auditlog
     */
    public static function create($event, DBObject $target) {
        $statLog = new self();
        
        $statLog->event = $event;
        $statLog->created = time();
        $statLog->target_type = get_class($target);
        
        switch ($statLog->target_type){
            case File::getClassName():
                $statLog->size = $target->size;
            break;
            case Transfer::getClassName():
                $tmpSize= 0;
                foreach ($target->files as $file){
                    $tmpSize += $file->size;
                }
                $statLog->size = $tmpSize;
            break;
            default:
                $statLog->size = 0;
                break;
        }
        
        $statLog->save();
        
        return $statLog;
    }
    
    /**
     * Getter
     * 
     * @param string $property property to get
     * 
     * @throws PropertyAccessException
     * 
     * @return property value
     */
    public function __get($property) {
        if(in_array($property, array(
            'id', 
            'event',
            'created',
            'target_type',
            'size',
        ))) return $this->$property;
        
        throw new PropertyAccessException($this, $property);
    }
}
