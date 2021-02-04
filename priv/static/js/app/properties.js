define(["ecomet_req","app/errordlg","app/dialogid","app/types"],
function(ecomet,errordlg,dialogid,types) { 
    
    return function (OID,Operation,Properties,OnSave){
        var editedProperties={};
        var dialogId=dialogid();
        var $btnMonitor;
        var subscriptionId="none";

        showDialog();
        for (var name in Properties){
            $('#grid'+dialogId).jqGrid('addRowData',name,{"name":name,"value":Properties[name]});
        }

        //==========================================================
        // Dialog window
        //==========================================================
        function showDialog(){
            var gridparams={
                "datatype": 'local',
                "autowidth":true,
                "height":"auto",
                "viewrecords":true,
                "gridview": true,
                "shrinkToFit":true,
                "colNames":["name","value"],
                "colModel":[
                    {"name":"name","index":"name","formatter":nameformatter},
                    {"name":"value","index":"value","formatter":valueformatter}
                ],
                "ondblClickRow":function(rowid,iRow,iCol,e){
                    if (subscriptionId=="none"){
                        editProperty(rowid,Properties[rowid]);
                    }
                }
            };
            $('body').append('<div id="dlg'+dialogId+'"><table id="grid'+dialogId+'"><tr><td/></tr></table></div>');
            var dlgButtons={"Save":saveChanges,"Cancel":cancelChanges};
            if (Operation=="edit"){
                dlgButtons["Monitor"]=monitor;
            }
            $('#dlg'+dialogId).dialog({
                "modal": false,
                "width":400,
                "title":OID+" "+Operation+" "+Properties['.name'].value,
                "buttons":dlgButtons
            });
            $('.ui-dialog-titlebar-close').hide();
            $('#grid'+dialogId).jqGrid(gridparams);
            $btnMonitor=$('#dlg'+dialogId+'~div.ui-dialog-buttonpane button').find('*:contains("Monitor")');
        }

        function nameformatter(name,options,rowObject){
            if (Properties[name].required=="true"){
                return name+" *";
            } else {
                return name;
            }
        }

        function valueformatter(value,options,rowObject){
            if (Operation=="create"){
                if (Properties[options.rowId]["default_value"]==".autoincrement"){
                    return ".autoincrement";
                }
            } 
            if (value.value=="none"){
                return types.getvalue(Properties[options.rowId]["default_value"]);
            }
            return types.getvalue(value.value);
        }

        function editProperty(Name,Value){
            if (Operation=="edit"){
                if (Properties[Name]["final"]=="true"){
                    if (Properties[Name].value!="none"){
                        errordlg("Property is final");
                        return;
                    }
                }
            }
            types.setvalue(Name,Value,function(NewValue){
                Properties[Name]["value"]=NewValue;
                editedProperties[Name]=Properties[Name];
                $('#grid'+dialogId).jqGrid('setCell',Name,'value',Properties[Name]);
            });
        }


        function saveChanges(){
            if (checkRequired()=="ok"){
                if (subscriptionId!="none"){
                    ecomet.unsubscribe(subscriptionId);
                }
                OnSave(editedProperties);
                delete editedProperties;
                $('#dlg'+dialogId).dialog("close");
                $('#dlg'+dialogId).remove();
            }
        }

        function checkRequired(){
            var result="ok";
            for (var name in Properties){
                if (Properties[name].required=="true"){
                    if (editedProperties[name]==undefined){
                        if (Operation=="create"){
                            if (Properties[name].value!="none"){
                                editedProperties[name]=Properties[name];
                            } else{
                                result="error";
                                break;
                            }
                        }
                    } else if(editedProperties[name].value=="none"){
                        result="error";
                        break;
                    }
                }
            }
            if (result=="error"){
                errordlg("Property "+name+" is required");
            }
            return result;
        }

        function cancelChanges(){
            delete editedProperties;
            if (subscriptionId!="none"){
                ecomet.unsubscribe(subscriptionId);
            }
            $('#dlg'+dialogId).dialog("close");
            $('#dlg'+dialogId).remove();
        }

        function monitor(){
            if (subscriptionId=="none"){
                var shureDialogId=dialogid();
                $('body').append('<div id="dlg'+shureDialogId+'"><div>All changes will be lost</div></div>');
                $('#dlg'+shureDialogId).dialog({
                    "modal": true,
                    "width":400,
                    "title":"Switch on monitoring?",
                    "buttons":{
                        "OK":function(){
                            editedProperties={};
                            subscriptionId=ecomet.subscribe("GET * from * WHERE .path='"+Properties[".folder"].value+"/"+Properties[".name"].value+"'",
                                function(createObject){
                                    for (var name in createObject.fields){
                                        Properties[name]["value"]=createObject.fields[name];
                                        $('#grid'+dialogId).jqGrid('setCell',name,'value',Properties[name]);
                                    }
                                },
                                function(editObject){
                                    for (var name in editObject.fields){
                                        Properties[name]["value"]=editObject.fields[name];
                                        $('#grid'+dialogId).jqGrid('setCell',name,'value',Properties[name]);
                                    }
                                },
                                function(deleteObject){
                                    errordlg("Object just deleted",cancelChanges);
                                },
                                function(Error){
                                    ecomet.unsubscribe(subscriptionId);
                                    subscriptionId="none";
                                    errordlg(Error);
                                }
                            );
                            $('#dlg'+shureDialogId).dialog("close");
                            $('#dlg'+shureDialogId).remove();
                            $btnMonitor.text("Offline");
                        },
                        "Cancel":function(){
                            $('#dlg'+shureDialogId).dialog("close");
                            $('#dlg'+shureDialogId).remove();
                        }
                    }
                });
                $('.ui-dialog-titlebar-close').hide();
            } else{
                $btnMonitor.text("Monitor");
                ecomet.unsubscribe(subscriptionId);
                subscriptionId="none";
            }
            
        }
    }
});