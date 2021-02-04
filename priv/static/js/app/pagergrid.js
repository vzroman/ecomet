define(["ecomet_req","app/types","app/errordlg","app/dialogid"],
function(ecomet,types,errordlg,dialogid) { 
    //================================================================ 
    //  Class methods
    //================================================================
    function readvalue(cellvalue, options, rowObject){
        return types.getvalue(cellvalue);
    }

    function remove(grid){
        grid.removeData();
        delete grid;
    }

    //================================================================ 
    //  Exported methods
    //================================================================
    function createGrid($element,params,OnReady){
        //================================================================ 
        //  Internal properties
        //================================================================
        var $grid;
        var tableContent={};
        var $container=[];
        var queryFields="";
        var currentPage=0;
        var tableparams={
            datatype: loaddata,
            autowidth:true,
            rowNum:30,
            rowList:[30,100,300],
            viewrecords: true,
            gridview: true,
            shrinkToFit:true,
            ondblClickRow:function(rowid,iRow,iCol,e){
                if (typeof onDblClick== 'function'){
                    onDblClick(tableContent[rowid]);
                }
            },
            onSelectRow:function(rowid,status,e){
                if (typeof onSelectRow== 'function'){
                    onSelectRow(tableContent[rowid],status);
                }
            },
            jsonReader : {
                cell: "fields",
                id: "oid"
            }
        };
        //================================================================ 
        //  Exported properties
        //================================================================
        var condition=params['condition'];
        var onDblClick=params['onDblClick'];
        var onSelectRow=params['onSelectRow'];
        
        // Fill column model for jqGrid
        var colModel=[];
        for (var i in params.fields){
            queryFields=queryFields+params.fields[i];
            if (params.fields[parseInt(i)+1]!=undefined){
                queryFields=queryFields+",";
            }
            colModel.push({name:params.fields[i],index:params.fields[i],formatter:readvalue});
        }
        if (params['hiddenFields']!=undefined){
            queryFields=queryFields+",";
            for (var i in params.hiddenFields){
                queryFields=queryFields+params.hiddenFields[i];
                if (params.hiddenFields[parseInt(i)+1]!=undefined){
                    queryFields=queryFields+",";
                }
            }
        }
        // Fill adjustable jqGrid params
        tableparams['height']=params['height'];
        tableparams['width']=params['width'];
        tableparams['colNames']=params['fields'];
        tableparams['colModel']=colModel;
        tableparams['multiselect']=params['multiselect'];
        // Load markup    
        $element.load("markup/pagergrid.html",function(){
            $container=$element.children('*[name="pagergrid"]');
            var tableId=dialogid();
            $grid=$container.children('*[name="body"]');
            $grid.attr("id","grid"+tableId);
            var $pager=$container.children('*[name="pager"]');
            $pager.attr("id","pager"+tableId);
            tableparams['pager']="#"+"pager"+tableId;
            $grid.jqGrid(tableparams);
            OnReady({
                "getCondition":getCondition,
                "setCondition":setCondition,
                "refresh":refresh,
                "onDblClick":onDblClick,
                "onSelectRow":onSelectRow,
                "getSelected":getSelected,
                "removeData":removeData,
                "resetPage":resetPage,
            });
        });

        // Loading data from ecomet
        function loaddata(postdata){
            clearTable();
            if (currentPage!=0){
                currentPage=postdata.page;
            } else{
                currentPage=1;
            }
            ecomet.find("GET "+queryFields+" from * WHERE "+condition+" PAGE "+currentPage+":"+postdata.rows+" ORDER BY .oid DESC",
                function(result){
                    for (var i in result.set){
                        tableContent[result.set[i].oid]=result.set[i];
                    }
                    $grid[0].addJSONData({
                        "total":Math.ceil(result.total/postdata.rows),
                        "page":currentPage,
                        "records":result.total,
                        "rows":result.set
                    });
                },
                function(ErrorText){errordlg(ErrorText);}
            );
        }
        // Clear table content
        function clearTable(){
            tableContent={};
            $grid.jqGrid('clearGridData',true);
        }

        function refresh(){
            $grid.trigger('reloadGrid');
        }
        //Remove data. Must be launched for removing table
        function removeData(){
            clearTable();
            $container.remove();
        }
        // return array of selected rows
        function getSelected(){
            var selectedObjects=[];
            if ($grid!=undefined){
                var selectedOIDs=$grid.jqGrid('getGridParam','selarrrow');
                for (var i in selectedOIDs){
                    selectedObjects.push(tableContent[selectedOIDs[i]]);
                }
            }
            return selectedObjects;
        }

        function getCondition(){
            return condition;
        }
        function setCondition(Value){
            resetPage();
            condition=Value;
        }

        function resetPage(){
            currentPage=0;
        }
    }

    // Return object according to require.js   
    return {
            "createGrid":createGrid,
            "remove":remove
    };
});