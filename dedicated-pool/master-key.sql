https://stackoverflow.com/questions/46373723/please-create-a-master-key-in-the-database-or-open-the-master-key-in-the-session

--on primary, output: master 
select name from sys.databases where is_master_key_encrypted_by_server=1

--on secondary, output: nothing...
select name from sys.databases where is_master_key_encrypted_by_server=1


--on secondary
drop certificate [BackupCertWithPK]
drop master key

--Skipped restore master key from file.
--Instead, I ran create master key with password.
create master key encryption by password = 'MyTest!Mast3rP4ss';

--verify by open/close.
open master key decryption by password = 'MyTest!Mast3rP4ss';
close master key;

--proceed to restore/create cert from file.
create cerfiticate [BackupCertWithPK] 
from file = '\\FS1\SqlBackups\SQL1\Donot_delete_SQL1-Primary_BackupCertWithPK.cer' 
with private key (file = '\\FS1\SqlBackups\SQL1\Donot_delete_SQL1-Primary_BackupCertWithPK.key' , decryption by password = 'key_Test!prim@ryP4ss') ; 

--on secondary, output: master, now there was hope again!
select name from sys.databases where is_master_key_encrypted_by_server=1

