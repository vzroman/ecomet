define(["app/dialogid","app/errordlg"],
function(dialogid,errordlg) { 
    return function (multiselect,OnOk){
        var dialogId=dialogid();
        var selected=[];
        $('body').append('<div id="dlg'+dialogId+'"><table id="grid'+dialogId+'"><tr><td/></tr></table></div>');
        $('#dlg'+dialogId).dialog({
            modal: true,
            width:400,
            title:"Select pattern",
            buttons:{
               "OK":onSelectPattern,
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
            "multiselect":multiselect,
            "viewrecords":true,
            "gridview": true,
            "shrinkToFit":true,
            "colNames":["pattern"],
            "colModel":[{"name":"pattern","index":"pattern"}],
            "onSelectRow":function(rowid,status,e){
                if (multiselect==true){
                    selected=[];
                    var selectedPatterns=$('#grid'+dialogId).jqGrid('getGridParam','selarrrow');
                    for (var pattern in selectedPatterns){
                        selected.push("/root/.patterns/"+selectedPatterns[pattern]);
                    }
                    if (selected.length==0){
                        $btnOK.button("disable");
                    } else{
                        $btnOK.button("enable");
                    }
                } else{
                    selected=["/root/.patterns/"+rowid];
                    $btnOK.button("enable");
                }
            }
        });

        ecomet.find("GET .name from * WHERE .folder=$oid('/root/.patterns')",
            function(result){
                for (var i in result.set){
                    $('#grid'+dialogId).jqGrid('addRowData',result.set[i].fields[".name"],{"pattern":result.set[i].fields[".name"]});  
                }
            },
            function(ErrorText){errordlg(ErrorText);}
        );

        function onSelectPattern(){
            OnOk(selected);
            $('#dlg'+dialogId).dialog("close");
            $('#dlg'+dialogId).remove();
        }

        function onCancel(){
            $('#dlg'+dialogId).dialog("close");
            $('#dlg'+dialogId).remove();
        }
        
    }

    
});