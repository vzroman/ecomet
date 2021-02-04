define(["app/dialogid"],
function(dialogid) { 
    // Return function according to require.js   
    return function(ErrorText,OnClose){
        var dialogId=dialogid();
        $('body').append('<div id="error'+dialogId+'">'+ErrorText+'</div>');
        $('#error'+dialogId).dialog({
            draggable: false,
            modal: true,
            buttons:{
                "OK":function(){
                    $('#error'+dialogId).dialog("close");
                    $('#error'+dialogId).remove();
                    if (typeof OnClose=='function'){
                        OnClose();
                    }
                }
            }
        });
        $('.ui-dialog-titlebar-close').hide(); 
    }
});