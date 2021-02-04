define([],
function() {
    var types;
    //================================================================ 
    //  Helper methods
    //================================================================
    function readvalue(cellvalue, options, rowObject){
        return types.getvalue(cellvalue);
    }
    //================================================================ 
    //  Exported methods
    //================================================================
    return function($dialog,ListValue){
        types=require("app/types");
        var dialogId=$dialog.attr("id");
        $dialog.append('<table id="grid'+dialogId+'"><tr><td/></tr></table>');
        var buttons = $dialog.dialog("option","buttons");
        buttons["Add"]=addElement;
        buttons["Delete"]=delElement;
        $dialog.dialog("option","buttons",buttons);
        var $btnDelete=$('#'+dialogId+'~div.ui-dialog-buttonpane button').find('*:contains("Delete")').parent();
        $btnDelete.button("disable");
        delete $dialog[0]["ListValue"];
        $dialog[0]["ListValue"]=ListValue;

        var gridparams={
            datatype: 'local',
            autowidth:true,
            height:'auto',
            viewrecords: true,
            gridview: true,
            multiselect:true,
            shrinkToFit:true,
            colNames:["values"],
            colModel:[
                {name:"values",index:"values",formatter:readvalue}
            ],
            ondblClickRow:function(rowid,iRow,iCol,e){
                types.setvalue(rowid,$dialog[0]["ListValue"]["value"][rowid],function(NewValue){
                    setProperty(rowid,NewValue);
                });
            },
            onSelectRow:function(){
                var selectedIDs=$('#grid'+dialogId).jqGrid('getGridParam','selarrrow');
                if (selectedIDs.length>0){
                    $btnDelete.button("enable");
                } else{
                    $btnDelete.button("disable");
                }
            },
        };
        $('#grid'+dialogId).jqGrid(gridparams);
        if ($dialog[0]["ListValue"].value!="none"){
            for (var i in $dialog[0]["ListValue"].value){
                $('#grid'+dialogId).jqGrid('addRowData',i,{"values":$dialog[0]["ListValue"].value[i]});
            }
        }

        function setProperty(Id,Value){
            $dialog[0]["ListValue"].value[Id]=Value;
            $('#grid'+dialogId).jqGrid('setCell',Id,'values',Value);
        }

        function addElement(){
            types.setvalue("+",{"type":$dialog[0]["ListValue"].subtype,"value":"none"},function(NewValue){
                if ($dialog[0]["ListValue"].value=="none"){
                    $dialog[0]["ListValue"].value=[];
                }
                var nextId=$dialog[0]["ListValue"].value.length;
                $('#grid'+dialogId).jqGrid('addRowData',nextId,{"values":NewValue});
                $dialog[0]["ListValue"].value.push(NewValue);
            });
        }

        function delElement(){
            var selectedIDs=$('#grid'+dialogId).jqGrid('getGridParam','selarrrow');
            for (var i in selectedIDs){
                $dialog[0]["ListValue"].value.splice(parseInt(selectedIDs[i]),1);
                $('#grid'+dialogId).jqGrid('delRowData',selectedIDs[i]);
            }
        }
    }
});