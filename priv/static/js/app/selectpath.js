define(["require","app/dialogid"],
function(require,dialogid) { 
    
    //================================================================ 
    //  Exported methods
    //================================================================
    return function (Title,currentPath,OnOk){
        var dialogId=dialogid();
        var navigatorGrid;
        var navigator=require("app/navigator");
        var $btnOK;
        var selectedPath;
        showDialog();
        //==========================================================
        // Dialog window
        //==========================================================
        function showDialog(){
            $('body').append('<div id="dlg'+dialogId+'"><div name="navigator"></div></div>');
            $('#dlg'+dialogId).dialog({
                modal: true,
                width:600,
                title:Title,
                buttons:{
                    "OK":onSelectPath,
                    "Cancel":onCancel,
                }
            });
            $('.ui-dialog-titlebar-close').hide();
            $btnOK=$('#dlg'+dialogId+'~div.ui-dialog-buttonpane button').find('*:contains("OK")').parent();
            $btnOK.button("disable");
            navigator(
                $('#dlg'+dialogId+' div[name="navigator"]'),
                {
                    "startPath":currentPath,
                    "fields":['.name','.pattern'],
                    "height":350,
                    "multiselect":false,
                    "onSelectRow":function(selectedObject){
                        selectedPath=selectedObject; 
                        $btnOK.button("enable");
                    },
                    "onChangeFolder":function(){
                        $btnOK.button("disable");
                    }
                },
                function(readyNavigator){
                    navigatorGrid=readyNavigator;
                }
            );
        }

        function onSelectPath(){
            OnOk(selectedPath.path);
            navigatorGrid.remove();
            $('#dlg'+dialogId).dialog("close");
            $('#dlg'+dialogId).remove();
        }

        function onCancel(){
            navigatorGrid.remove();
            $('#dlg'+dialogId).dialog("close");
            $('#dlg'+dialogId).remove();
        }
        
    }

    
});