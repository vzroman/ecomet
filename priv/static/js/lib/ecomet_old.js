/* 
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

var ecomet;
if (!ecomet) {
    ecomet = {};
}

(function () {
    // Web socket
    var connection=undefined;
    // Array of actual queries
    var actions={};
    var actionId=0;

    if (typeof ecomet.connect !== 'function') {
        ecomet.connect = function (IP,Port,Protocol,OnOk,OnError,OnClose) {
            try {
                // define protocol
                var wsProtocol="ws:";
                if (Protocol=="https:"){
                    wsProtocol="wss:";
                }
                connection=new WebSocket(wsProtocol+"//"+IP+":"+Port+"/websocket");
                connection.onopen=function(){ OnOk() };
                connection.onmessage=function(event){ on_receive(event.data) };
                connection.onclose=function(){
                    connection=undefined;
                    actions={};
                    actionId=0;
                    OnClose()
                };
            } catch(Error){
                OnError(Error);
            }
            
        }
    }

    if (typeof ecomet.login !== 'function') {
        ecomet.login = function (Login,Pass,OnOk,OnError,Timeout) {
            return action("login",{"login":Login,"pass":Pass},OnOk,OnError,Timeout,-1);
        }
    }

    if (typeof ecomet.create_object !== 'function') {
        ecomet.create_object = function (Fields,OnOk,OnError,Timeout) {
            return action("create",{"fields":Fields},OnOk,OnError,Timeout,-1);
        }
    }

    if (typeof ecomet.edit_object !== 'function') {
        ecomet.edit_object = function (OID,Fields,OnOk,OnError,Timeout) {
            return action("edit",{"oid":OID,"fields":Fields},OnOk,OnError,Timeout,-1);
        }
    }

    if (typeof ecomet.delete_object !== 'function') {
        ecomet.delete_object = function (OID,OnOk,OnError,Timeout) {
            return action("delete",{"oid":OID},OnOk,OnError,Timeout,-1);
        }
    }

    if (typeof ecomet.find !== 'function') {
        ecomet.find = function (QueryString,OnOk,OnError,Timeout) {
            return action("query",{"query_string":QueryString},OnOk,OnError,Timeout,-1);
        }
    }

    if (typeof ecomet.subscribe !== 'function') {
        ecomet.subscribe = function (QueryString,OnCreate,OnEdit,OnDelete,OnError) {
            var id=actionId++;
            var OnOk=function(result){
                if (result!="ok"){
                    if ((result.oper=="create")&&(typeof OnCreate==='function')){
                        if (typeof OnCreate==='function'){ OnCreate(result); }
                    }
                    if ((result.oper=="edit")&&(typeof OnEdit==='function')){
                        OnEdit(result);
                    }
                    if ((result.oper=="delete")&&(typeof OnDelete==='function')){
                        OnDelete(result);
                    }
                }
            };
            return action("query",{"query_string":"SUBSCRIBE CID="+id+" "+QueryString},OnOk,OnError,undefined,id);
        }
    }

    if (typeof ecomet.unsubscribe !== 'function') {
        ecomet.unsubscribe = function (id,OnOk,OnError) {
            return action("unsubscribe",{"none":"none"},
                function(){ 
                    delete actions[id];
                    if (typeof OnOk==='function') { OnOk(); }
                },
                function(ErrorText) {
                    delete actions[id];
                    if (typeof OnError==='function') { OnError(ErrorText); }
                },
            5000,id);
        }
    }

    if (typeof ecomet.application !== 'function') {
        ecomet.application = function (module,efunction,function_params,OnOk,OnError,Timeout) {
            var requestParams={
                "module":module,
                "function":efunction,
                "function_params":function_params
            };
            return action("application",requestParams,OnOk,OnError,Timeout,-1);
        }
    }

    function action(Type,Params,OnOk,OnError,Timeout,definedId){
        if (connection==undefined) { throw "No connection"; }
        var id=definedId;
        if (id==-1){id=actionId++;}

        actions[id]={
            "OnOk":OnOk,
            "OnError":OnError,
            "delete":(definedId==-1)
        };

        connection.send(JSON.stringify({"id":id,"action":Type,"params":Params}));
        if ((Timeout!=undefined)&&(typeof OnError==='function')){
            setTimeout(function(){
                if ((actions[id]!=undefined)&&(typeof actions[id]["OnError"]==='function')){
                    OnError("timeout");
                    delete actions[id];
                }
            },Timeout);
        }
        return id;
    }

    function on_receive(data){
        var response=JSON.parse(data);
        if (actions[response.id]==undefined){ return; }
        if ((response.type=="error")&&(typeof actions[response.id]["OnError"]==='function')){
            actions[response.id].OnError(response.result);
        } else if (typeof actions[response.id]["OnOk"]==='function') {
            actions[response.id].OnOk(response.result);
        }
        if ((actions[response.id]!=undefined)&&(actions[response.id]["delete"]==true)){
            delete actions[response.id];
        }
    }
    
}());

if (typeof define == 'function') {
    define(ecomet);
}