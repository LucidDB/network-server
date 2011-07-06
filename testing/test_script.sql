-- after installing the network jar, you can edit the repository path
-- (/home/kevin/git_repos/dynamo_network/server)
-- in the three places it occurs below and try
-- running this script!
!set outputformat vertical
!set color true
call sys_network.add_repository('file:///home/kevin/git_repos/dynamo_network/server');


-- verify we have nothing downloaded, repo can be accessed
select * from sys_network.repositories;
select * from sys_network.packages;
-- fun with removing
call sys_network.remove_repository('file:///home/kevin/git_repos/dynamo_network/server');
select * from sys_network.repositories;
select * from sys_network.packages;
-- add it back
call sys_network.add_repository('file:///home/kevin/git_repos/dynamo_network/server');

-- in our sample metadata, jython has a pretend dependency on couchdb, so installing
-- pkg Jython should grab couchdb too

call sys_network.net_install('Jython');
select * from sys_network.packages;
select * from sys_root.dba_jars where schema_name = 'SYS_NETWORK';

-- consequently, couchdb has a reverse-dep on Jython, so if it's uninstalled, so should
-- Jython

call sys_network.net_uninstall('CouchDB');
select * from sys_network.packages;
select * from sys_root.dba_jars where schema_name = 'SYS_NETWORK';

-- they haven't actually been removed from disk yet.
call sys_network.net_remove('CouchDB');
select * from sys_network.packages;

