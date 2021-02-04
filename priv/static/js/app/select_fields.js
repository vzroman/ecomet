define(["app/dialogid","app/errordlg"],
function(dialogid,errordlg) { 
    return function (Pattern,OnOk){
        var dialogId=dialogid();
        var selected=[];
        $('body').append('<div id="dlg'+dialogId+'"><table id="grid'+dialogId+'"><tr><td/></tr></table></div>');
        $('#dlg'+dialogId).dialog({
            modal: true,
            width:400,
            title:"Select fields",
            buttons:{
               "OK":onSelectFields,
               "Cancel":onCancel,
            }
        });
        $('.ui-dialog-titlebar-close').hide();
        $btnOK=$('#dlg'+dialogId+'~div.ui-dialog-buttonpane button').find('*:contains("OK")').parent();
        $btnOK.button("disable");

        $('#grid'+dialogId).jqGrid({
            "datatype": 'local',
            "autowidth":true,
            "height":400,
            "multiselect":true,
            "viewrecords":true,
            "gridview": true,
            "shrinkToFit":true,
            "colNames":["field","type"],
            "colModel":[{"name":"field","index":"field"},{"name":"type","index":"type"}],
            "onSelectRow":function(rowid,status,e){
                    selected=[];
                    var selectedFields=$('#grid'+dialogId).jqGrid('getGridParam','selarrrow');
                    for (var field in selectedFields){
                        selected.push(selectedFields[field]);
                    }
                    if (selected.length==0){
                        $btnOK.button("disable");
                    } else{
                        $btnOK.button("enable");
                    }
            }
        });

        ecomet.find("GET .name,type from * WHERE .folder=$oid('"+Pattern+"')",
            function(result){
                for (var i in result.set){
                    $('#grid'+dialogId).jqGrid('addRowData',result.set[i].fields[".name"],{"field":result.set[i].fields[".name"],"type":result.set[i].fields["type"]});
                }
            },
            function(ErrorText){errordlg(ErrorText);}
        );

        function onSelectFields(){
            OnOk(Pattern,selected);
            $('#dlg'+dialogId).dialog("close");
            $('#dlg'+dialogId).remove();
        }

        function onCancel(){
            $('#dlg'+dialogId).dialog("close");
            $('#dlg'+dialogId).remove();
        }
        
    }
    
});