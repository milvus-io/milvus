alter table Tables add column 'owner_table' TEXT DEFAULT '' NOT NULL;
alter table Tables add column 'partition_tag' TEXT DEFAULT '' NOT NULL;
alter table Tables add column 'version' TEXT DEFAULT '0.6.0' NOT NULL;
update Tables set version='0.6.0';
