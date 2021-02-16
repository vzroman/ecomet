define(["ecomet_req","app/dialogid","app/selectpath","app/editlist","app/errordlg"],
function(ecomet,dialogid,selectpath,editlist,errordlg) {
    function getvalue(value){
        if (value=="undefined_field"){
            return "undefined";
        } else if (value==null){
            return "";
        } else if (Array.isArray(value)){
            var valueArray="";
            for (var i in value){
                valueArray=valueArray+getvalue(value[i]);
                if (value[parseInt(i)+1]!=undefined){
                    valueArray=valueArray+",";
                }
            }
            return "["+valueArray+"]";
        } else{
            return value;
        }
    }

    function setvalue(Name,Value,OnChange){
        var dialogId=dialogid();
        $('body').append('<div id="dlg'+dialogId+'"></div>');
        $('#dlg'+dialogId).dialog({
            modal: true,
            width:400,
            title:Name,
            buttons:{
                "OK":function(){
                    var NewValue=OnOk(Value.type,$('#dlg'+dialogId));
                    OnChange(NewValue);
                    $('#dlg'+dialogId).dialog("close");
                    $('#dlg'+dialogId).remove();
                },
                "Cancel":function(){
                    $('#dlg'+dialogId).dialog("close");
                    $('#dlg'+dialogId).remove();
                }
            }
        });
        $('.ui-dialog-titlebar-close').hide();
        getContent($('#dlg'+dialogId),Value);
    }
    function getContent($dialog,Value){
        if (Value.type=="list"){
            return getContentList($dialog,Value);
        } else if (Value.type=="link"){
            return getContentLink($dialog,Value);
        } else if (Value.type=="integer"){
            return getContentInteger($dialog,Value);
        } else if (Value.type=="float"){
            return getContentFloat($dialog,Value);
        } else if (Value.type=="bool"){
            return getContentBool($dialog,Value);
        } else if (Value.type=="binary"){
            return getContentBinary($dialog,Value);
        } else if (Value.type=="string"){
            return getContentString($dialog,Value);
        }
    }

    function getContentString($dialog,Value){
        $dialog.append('<textarea rows="1" cols="20" name="value">'+getvalue(Value.value)+'</textarea>');
    }

    function getContentInteger($dialog,Value){
        $dialog.append('<INPUT TYPE=TEXT NAME="value" value="'+getvalue(Value.value)+'" style="width:350px">');
    }

    function getContentFloat($dialog,Value){
        $dialog.append('<INPUT TYPE=TEXT NAME="value" value="'+getvalue(Value.value)+'" style="width:350px">');
    }

    function getContentBool($dialog,Value){
        var markup='<select NAME="value">';
        var ValueValue=getvalue(Value.value);
        if (ValueValue=="true"){
            markup=markup+'<option value="true" selected>true</option><option value="false">false</option><option value="none">none</option>';
        } else if(ValueValue="false"){
            markup=markup+'<option value="true">true</option><option value="false" selected>false</option><option value="none">none</option>';
        } else{
            markup=markup+'<option value="true">true</option><option value="false">false</option><option value="none" selected>none</option>';
        }
        $dialog.append(markup+'</select>');
    }

    function getContentBinary($dialog,Value){
        $dialog.append('<INPUT TYPE=TEXT NAME="value" value="'+getvalue(Value.value)+'" style="width:350px">');
    }

    function getContentLink($dialog,Value){
        var markup='<span><INPUT TYPE=TEXT NAME="value" value="'+Value.value+'" style="width:300px"></span>';
        markup=markup+'<span><div name="selectpath">..</div></span>';
        $dialog.append(markup);
        var $currentPath=$dialog.children('span').children('INPUT[name="value"]');
        var $selectButton=$dialog.children('span').children('div[name="selectpath"]');

        $selectButton.button();
        $selectButton.click(function(){
            getStartPath(Value.value,function(startPath){
                selectpath("Link",startPath,function(NewValue){
                        $currentPath.val(NewValue);
                    }
                );
            });
        });
    }

    function getStartPath(Path,OnResult){
        if (Path==""){
            OnResult("/root");
        }
        ecomet.find("GET .name,.folder,only_patterns from * WHERE .path='"+Path+"'",
            function(result){
                if (result.total==1){
                    if (result.set[0].fields['only_patterns']!="undefined_field"){
                        OnResult(Path);
                    } else{
                        getStartPath(Path.substring(0,Path.lastIndexOf("/")),OnResult);
                    }
                } else{
                    getStartPath(Path.substring(0,Path.lastIndexOf("/")),OnResult);
                }
            },
            function(ErrorText){
                OnResult("/root");
            }
        );
    }
    
    function getContentList($dialog,Value){
        editlist($dialog,Value);
    }


    function OnOk(Type,$dialog){
        if (Type=="string"){
            return readValueString($dialog);
        } else if (Type=="integer"){
            return readValueInteger($dialog);
        } else if (Type=="float"){
            return readValueFloat($dialog);
        } else if (Type=="bool"){
            return readValueBool($dialog);
        } else if (Type=="binary"){
            return readValueBinary($dialog);
        } else if (Type=="link"){
            return readValueLink($dialog);
        } else if (Type=="list"){
            return readValueList($dialog);
        }
    }

    function readValueString($dialog){
        var value=$dialog.children("*[name='value']").val();
        if (value==""){
            return null;
        } else{
            return value;
        }
    }

    function readValueInteger($dialog){
        var value=$dialog.children("*[name='value']").val();
        if (value==""){
            return null;
        } else{
            value=parseInt(value);
            if (isNaN(value)){
                errordlg("Invalid integer");
                return null;
            }else{
                return value;
            }
        }
    }

    function readValueFloat($dialog){
        var value=$dialog.children("*[name='value']").val();
        if (value==""){
            return null;
        } else{
            value=parseFloat(value);
            if (isNaN(value)){
                errordlg("Invalid float");
                return null;
            }else{
                return value;
            }
            
        }
    }

    function readValueBool($dialog){
        var value=$dialog.children("*[name='value']").val();
        return value;
    }

    function readValueBinary($dialog){
        var value=$dialog.children("*[name='value']").val();
        if (value==""){
            return null;
        } else{
            return value;
        }
    }

    function readValueLink($dialog){
        var value=$dialog.find("*[name='value']").val();
        if (value==""){
            return null;
        } else{
            return value;
        }
    }

    function readValueList($dialog){
        var value=$dialog[0]["ListValue"].value;
        if (Array.isArray(value)){
            if (value.length==0){
                return null;
            } else{
                return value;
            }
        } else{
            return null;
        }
    }

    return {"getvalue":getvalue,"setvalue":setvalue}
});
