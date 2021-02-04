

define(["loadcss", "ecomet_req","app/errordlg"],
function(loadcss,ecomet,errordlg) {
    var OnLoggedOn="";
    var OnClose="";
    var $nick="";
    var $pass="";
    return function(IP,Port,Protocol,OnOk,OnDisconnect) {
        OnLoggedOn=OnOk;
        OnClose=OnDisconnect;
        markup(function(){connect(IP,Port,Protocol);});
    }
    function markup(connect){
        clean_markup();
        var markup='';
        markup=markup+'<div id="ecometlogin">';
        markup=markup+' <div name="connection"/>';
        markup=markup+' <div name="error"/>';
        markup=markup+' <div name="loginform" title="Authorization" style="text-align: right;margin-top: 7px;">';
        markup=markup+'     <FORM>';
        markup=markup+'         <div name="nickbox" style="margin-bottom:5px;">';
        markup=markup+'             <span style="font-size: 0.8em;">Login: </span>';
        markup=markup+'             <span style="font-size: 0.8em;"><INPUT TYPE=TEXT NAME="nick" class="ctrl"></span>';
        markup=markup+'         </div>';
        markup=markup+'         <div name="passbox" style="margin-bottom:5px;">';
        markup=markup+'             <span style="font-size: 0.8em;">Password: </span>';
        markup=markup+'             <span style="font-size: 0.8em;"><INPUT TYPE=PASSWORD NAME="pass" class="ctrl"></span>';
        markup=markup+'         </div>';
        markup=markup+'     </FORM>';
        markup=markup+' </div>';
        markup=markup+'</div>';
        $("body").append(markup);
        $connection=$('#ecometlogin *[name="connection"]');
        $nick=$('*[name="loginform"] *[name="nick"]');
        $pass=$('*[name="loginform"] *[name="pass"]');
        $connection.dialog({
            autoOpen: false,
            draggable: false,
            resizable: false,
            modal: true,

        });
        connect();
    }

    function clean_markup(){
        $('#ecometlogin').remove();
    }
    function connect(IP,Port,Protocol){
        $connection.text('Connecting...');
        $connection.dialog("option",{buttons:{}});
        $connection.dialog("open");
        ecomet.connect(IP,Port,Protocol,
            showdialog,
            function(ErrorText){
                $connection.text(ErrorText);
                $connection.dialog("option",{
                    buttons:{"Try again":function(){connect(IP,Port,OnClose);}}
                });
            },
            OnClose
        );
    }
    function showdialog(){
        $connection.dialog("close");
        $('*[name="loginform"]').dialog({
            draggable: false,
            resizable: false,
            modal: true,
            width: 340,
            buttons:{
            "Enter":login
            }
        });
        $('.ui-dialog-titlebar-close').hide();
        $nick.focus();
        var $ctrls=$('.ui-button, .ctrl');
        $('*[name="loginform"] *[name="nick"], *[name="loginform"] *[name="pass"]').keypress(function(event){
            if(event.which==13){
                $this=$(this);
                if($this.val()==''){
                    var ErrorText="";
                    if ($this.attr("name")=='nick'){
                        ErrorText='Please enter Login';
                    }
                    if ($this.attr("pass")=='pass'){
                        ErrorText='Please enter password';
                    }
                    errordlg(ErrorText,function(){$this.focus();});
                }
                $ctrls[$ctrls.index(this)+1].focus();
                event.preventDefault();
            }
        });
    }
    function login(){
        if ($nick.val()==''){
            errordlg('Please enter Login',function(){$nick.focus();});
            return;
        }
        if ($pass.val()==''){
            errordlg('Please enter password',function(){$pass.focus();});
            return;
        }
        ecomet.login($nick.val(),$pass.val(),
            function(OkText){
                clean_markup();
                OnLoggedOn();
            },
            function(ErrorText){
                $nick.val('');
                $pass.val('');
                errordlg(ErrorText,function(){$nick.focus();});
            }
        );
    }
});
