define(["ecomet_req","app/dialogid","app/errordlg","app/types"],
function(ecomet,dialogid,errordlg,types) { 
    return function (){
        var dialogId=dialogid();
        var subscriptionId=null;
        $('body').append('<div id="dlg'+dialogId+'"></div>');
        $('#dlg'+dialogId).dialog({
            modal: false,
            width:700,
            title:"Query",
            buttons:{
               "Close":function(){
                    if (subscriptionId!=null){
                        ecomet.unsubscribe(subscriptionId);
                    }
                    $('#grid'+dialogId).jqGrid('clearGridData',true);
                    $('#grid'+dialogId).remove();
                    $('#dlg'+dialogId).dialog("close");
                    $('#dlg'+dialogId).remove();
               }
            }
        });
        $('.ui-dialog-titlebar-close').hide();
        
        var markup='<div><div style="display:inline-block;vertical-align: middle;"><textarea rows="1" cols="70" name="querytext"></textarea></div>';
        markup=markup+'<div name="queryrun" style="display:inline-block">Run</div></div>';
        $('#dlg'+dialogId).append(markup);
        var $querytext=$('#dlg'+dialogId).find('textarea[name="querytext"]');
        var $queryrun=$('#dlg'+dialogId).find('div[name="queryrun"]');

        $queryrun.button();
        $queryrun.click(function(){
            if (subscriptionId!=null){
                ecomet.unsubscribe(subscriptionId);
                subscriptionId=null;
            }
            $('#grid'+dialogId).jqGrid('clearGridData',true);
            $('#grid'+dialogId).jqGrid('GridDestroy');
            var query=getQuery();
            if (query.type=="GET"){
                ecomet.find(query.text,
                    function(result){
                        if (result.total==0){
                            errordlg("No objects found");
                        } else{
                            var gridColumns=buildGrid(result.set[0].fields);
                            var $grid=$('#grid'+dialogId);
                            for (var i in result.set){
                                var cells={};
                                for (var col in gridColumns){
                                    cells[gridColumns[col]]=result.set[i].fields[gridColumns[col]];
                                }
                                $grid.jqGrid('addRowData',result.set[i].oid,cells);
                            }
                        }
                    },
                    function(ErrorText){errordlg(ErrorText);}
                );
            } else if(query.type=="SUBSCRIBE"){
                var gridColumns=null;
                var $grid;
                subscriptionId=ecomet.subscribe(query.text,
                    function(createObject){
                        if (gridColumns=null){
                            gridColumns=buildGrid(createObject.fields);
                            $grid=$('#grid'+dialogId);
                        }
                        var cells={};
                        for (var i in gridColumns){
                            cells[gridColumns[i]]=createObject.fields[gridColumns[i]];
                        }
                        $grid.jqGrid('addRowData',createObject.oid,cells);
                    },
                    function(editObject){
                        for (var name in editObject.fields){
                            $grid.jqGrid('setCell',editObject.oid,name,editObject.fields[name]);
                        }
                    },
                    function(deleteObject){
                        $grid.jqGrid('delRowData',deleteObject.oid);
                    },
                    function(Error){
                        ecomet.unsubscribe(subscriptionId);
                        subscriptionId=null;
                        errordlg(Error);
                    }
                );
            }
        });
        
        function buildGrid(fields){
            var colNames=[];
            var colModel=[];
            for (var name in fields){
                colNames.push(name);
                colModel.push({"name":name,"index":name,"formatter":function(value){return types.getvalue(value);}});
            }
            $('#dlg'+dialogId).append('<table id="grid'+dialogId+'"><tr><td/></tr></table>');
            $('#grid'+dialogId).jqGrid({
                "datatype": 'local',
                "autowidth":true,
                "height":"auto",
                "multiselect":false,
                "viewrecords":true,
                "gridview": true,
                "shrinkToFit":true,
                "colNames":colNames,
                "colModel":colModel
            });
            return colNames;
        }

        function getQuery(){
            var queryText=$querytext.val();
            var SubscribeQuery=queryText.match("^SUBSCRIBE\\s+CID\\s*=\\s*\\w+\\s+(.*)");
            if (Array.isArray(SubscribeQuery)){
                return {"type":"SUBSCRIBE","text":SubscribeQuery[1]};
            } else{
                return {"type":"GET","text":queryText};
            }
        }

        
    }

    
});