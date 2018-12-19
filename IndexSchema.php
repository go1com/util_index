<?php

namespace go1\util_index;

use Doctrine\DBAL\Schema\Schema;

class IndexSchema
{
    public static function install(Schema $schema)
    {
        if (!$schema->hasTable('index_history')) {
            $table = $schema->createTable('index_history');
            $table->addColumn('type', 'string');
            $table->addColumn('id', 'integer');
            $table->addColumn('status', 'smallint');
            $table->addColumn('data', 'blob', ['comment' => 'We should store the full comment if the entity is failed to be indexed.']);
            $table->addColumn('timestamp', 'integer');
            $table->addIndex(['type']);
            $table->addIndex(['id']);
            $table->addIndex(['timestamp']);
            $table->addIndex(['status']);
        }

        if (!$schema->hasTable('index_kv')) {
            $table = $schema->createTable('index_kv');
            $table->addColumn('name', 'string');
            $table->addColumn('value', 'blob');
            $table->addColumn('timestamp', 'integer', ['default' => 0]);
            $table->setPrimaryKey(['name']);
            $table->addIndex(['name']);
            $table->addIndex(['timestamp']);
        }

        if (!$schema->hasTable('index_task')) {
            $table = $schema->createTable('index_task');
            $table->addColumn('id', 'integer', ['unsigned' => true, 'autoincrement' => true]);
            $table->addColumn('title', 'string');
            $table->addColumn('instance', 'string', ['notnull' => false]);
            $table->addColumn('status', 'integer');
            $table->addColumn('percent', 'integer');
            $table->addColumn('author_id', 'integer');
            $table->addColumn('created', 'integer', ['default' => 0]);
            $table->addColumn('updated', 'integer', ['default' => 0]);
            $table->addColumn('data', 'blob');
            $table->setPrimaryKey(['id']);
            $table->addIndex(['title']);
            $table->addIndex(['instance']);
            $table->addIndex(['status']);
            $table->addIndex(['percent']);
            $table->addIndex(['author_id']);
            $table->addIndex(['created']);
            $table->addIndex(['updated']);
        }

        if (!$schema->hasTable('index_task_item')) {
            $table = $schema->createTable('index_task_item');
            $table->addColumn('id', 'integer', ['unsigned' => true, 'autoincrement' => true]);
            $table->addColumn('task_id', 'integer');
            $table->addColumn('handle', 'string');
            $table->addColumn('offset', 'integer');
            $table->addColumn('offset_id', 'integer');
            $table->addColumn('status', 'integer');
            $table->setPrimaryKey(['id']);
            $table->addUniqueIndex(['task_id', 'handle', 'offset']);
            $table->addIndex(['task_id']);
            $table->addIndex(['handle']);
            $table->addIndex(['offset']);
            $table->addIndex(['offset_id']);
            $table->addIndex(['status']);
        }

        // add data for index_task_item
        $table = $schema->getTable('index_task_item');
        if ($table->hasColumn('handle')) {
            $table->dropColumn('handle');
            $table->addColumn('handler', 'string');
        }
        !$table->hasColumn('data') && $table->addColumn('data', 'blob');
        if (!$table->hasColumn('updated')) {
            $table->addColumn('updated', 'integer', ['default' => 0]);
            $table->addIndex(['updated'], 'idx_index_task_item_updated');
        }
    }
}
