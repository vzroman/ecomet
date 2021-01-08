ecomet
=====

An OTP application

Build
-----

    $ rebar3 compile
    
 Login in
 ----------------
 ecomet:login(<<"system">>,<<"111111">>).
 
 Logout
 ----------------
 ecomet:logout().
    
 Create a new node
 -----------------
 ecomet:query("insert .name='new_node@my_domain.com', .folder=$oid('/root/.nodes'), .pattern=$oid('/root/.patterns/.node')").

Query configured nodes
-----------------------
ecomet:query("get .name, is_ready from * where .pattern=$oid('/root/.patterns/.node')").

Create a new database
 -----------------
 ecomet:query("insert .name='new_database', .folder=$oid('/root/.databases'), .pattern=$oid('/root/.patterns/.database')").
