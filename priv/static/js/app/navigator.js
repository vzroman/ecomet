

define(["loadcss", "ecomet_req","app/errordlg","app/pagergrid"],
function(loadcss,ecomet,errordlg,pagergrid) {
    return function($container,params,OnReady){
        var $path="";
        var $pathup="";
        var $pathroot="";
        var $table="";
        var grid="";
        var onChangeFolder=params['onChangeFolder'];
        var currentFolder={
            "path":"",
            "folder":""
        };
        loadcss("css/ecomet/navigator.css");
        $container.load("markup/navigator.html",function(){
            //Bind jquery objects
            $path=$container.find('*[name="pathbar"] *[name="pathtext"]');
            $pathup=$container.find('*[name="pathbar"] *[name="up"]');
            $pathroot=$container.find('*[name="pathbar"] *[name="root"]');
            $table=$container.find('*[name="navigatorgrid"]');
            // Init jquery objects
            $pathup.button();
            $pathup.click(upFolder);
            $pathroot.button();
            $pathroot.click(rootFolder);
            var hiddenFields;
            if (params['hiddenFields']==undefined){
                hiddenFields=[];
            } else{
                hiddenFields=params['hiddenFields'];
            }
            if (params['fields'].indexOf(".name")==-1){
                hiddenFields.push(".name");
            }
            if (params['fields'].indexOf(".folder")==-1){
                hiddenFields.push(".folder");
            }
            pagergrid.createGrid(
                $table,
                {
                    "condition":".folder='dummy'",
                    "fields":params['fields'],
                    "hiddenFields":hiddenFields,
                    "height":params['height'],
                    "multiselect":params['multiselect'],
                    "onDblClick":downFolder,
                    "onSelectRow":function(Object,status){
                        if (typeof params['onSelectRow']== 'function'){
                            params['onSelectRow']({"path":currentFolder.path+"/"+Object.fields[".name"],"object":Object},status);
                        }
                    }
                },
                function(readyGrid){
                    grid=readyGrid;
                    setPath(params['startPath']);
                    OnReady({
                        "getPath":getPath,
                        "setPath":setPath,
                        "getSelected":getSelected,
                        "getFilter":getFilter,
                        "setFilter":setFilter,
                        "remove":remove,
                        "refresh":refresh
                    });
                }
            );
        });
        function getPath(){
            return currentFolder.path;
        }
        function setPath(Path){
            findFolder(Path,
                function(Folder){
                    currentFolder.path=Path;
                    currentFolder.name=Folder.fields['.name'];
                    grid.setCondition(".folder=$oid('"+Folder.oid+"')");
                    grid.refresh();
                    $path.text(currentFolder.path);
                    if (typeof onChangeFolder=='function'){
                        onChangeFolder(Path);
                    }
                },
                function(){
                    errordlg("Invalid folder: "+Path);
                    setPath("/root");
                }
            );
        }
        
        function getSelected(){
            var resultArray=[];
            var selectedArray=grid.getSelected();
            for (var i in selectedArray) {
                resultArray.push({
                    "path":currentFolder.path+"/"+selectedArray[i].fields['.name'],
                    "object":selectedArray[i]
                });
            }
            return resultArray;
        }
    
        function getFilter(){
            return "ok";
        }
    
        function setFilter(Filter){
            return "ok";
        }

        function remove(){
            pagergrid.remove(grid);
        }
        
        function refresh(){
            grid.refresh();
        }
        function findFolder(Path,OnOk,OnError){
            ecomet.find("GET .name,.folder from * WHERE AND(.path='" +Path+ "', .pattern=$oid('/root/.patterns/.folder'))",
                function(result){
                    if (result.total==1){
                        OnOk(result.set[0]);
                    } else{
                        OnError();
                    }
                },
                function(ErrorText){OnError();}
            );
        }
        //==========================================================
        //  Navigation
        //==========================================================
        function rootFolder(){
            setPath("/root");
        }
        function downFolder(Object){
            var NewPath=currentFolder.path+"/"+Object.fields['.name'];
            setPath(NewPath);
        }
    
        function upFolder(){
            if (currentFolder.path!="/root"){
                var NewPath=currentFolder.path.substring(0,currentFolder.path.length-currentFolder.name.length-1);
                setPath(NewPath);
            };
        }
    }
});
