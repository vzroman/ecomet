ecomet
=====

An OTP application

Build
-----

    $ rebar3 compile
    
 Create a new node
 -----------------
 ecomet:query("insert .name='new_node@my_domain.com', .folder=$oid('/root/.nodes'), .pattern=$oid('/root/.patterns/.node')").
