requirejs.config({
    baseUrl: 'js/lib',
    paths: {
        app: '../app'
    }
});

requirejs([
    'ecomet_req',
    'app/login',
    'loadcss',
    'app/errordlg',
    'app/navigator',
    'app/properties',
    'app/selectpattern',
    'app/query'],
    function (
        ecomet,
        login,
        loadcss,
        errordlg,
        navigator,
        properties,
        selectpattern,
        query) {

        var $btncreate = "";
        var $btnedit = "";
        var $btndelete = "";
        var $btnquery = "";
        var $btnexport = "";
        var $btnimport = "";
        var navigatorGrid = "";

        //==========================================================
        //  Login procedure
        //==========================================================
        ecomet_login();
        function ecomet_login() {
            login(location.hostname, location.port, location.protocol,
                function () {
                    draw();
                },
                function () { setTimeout(ecomet_login, 1000); }
            );
        }


        //==========================================================
        //  Control panel
        //==========================================================
        function configButtons() {
            var selectedCount = navigatorGrid.getSelected().length;
            if (selectedCount > 0) {
                $btnedit.button("enable");
                $btndelete.button("enable");
            } else {
                $btnedit.button("disable");
                $btndelete.button("disable");
            }
        }

        function createObject() {
            var Folder = navigatorGrid.getPath();
            selectpattern(false, function (SelectedArr) {
                var Pattern = SelectedArr.pop();
                getPatternFields(Pattern, function (PatternFields) {
                    var ObjectFields = {};
                    for (var name in PatternFields) {
                        ObjectFields[name] = {};
                        if (name == ".folder") {
                            ObjectFields[".folder"]["value"] = Folder;
                        } else if (name == ".pattern") {
                            ObjectFields[".pattern"]["value"] = Pattern;
                        } else {
                            ObjectFields[name]["value"] = PatternFields[name]["default"];
                        }
                        ObjectFields[name]["type"] = PatternFields[name]["type"];
                        ObjectFields[name]["subtype"] = PatternFields[name]["subtype"];
                        ObjectFields[name]["required"] = PatternFields[name]["required"];
                        ObjectFields[name]["default"] = PatternFields[name]["default"];
                    }
                    properties("", "create", ObjectFields, function (filledProperties) {
                        var fields = { ".folder": Folder, ".pattern": Pattern };
                        for (var name in filledProperties) {
                            fields[name] = filledProperties[name].value;
                        }
                        ecomet.create_object(fields, function () { }, function (ErrorText) {
                            errordlg(ErrorText);
                        });
                        navigatorGrid.refresh();
                    });
                });
            });
        }

        function editObject() {
            var selectedArray = navigatorGrid.getSelected();
            for (var i in selectedArray) {
                editDialog(selectedArray[i].object);
            }
        }

        function editDialog(Object) {
            ecomet.find("GET * from * WHERE .path='" + Object.fields['.folder'] + "/" + Object.fields['.name'] + "'",
                function (result) {
                    var objectProperties = result.set.pop().fields;
                    getPatternFields(Object.fields['.pattern'], function (PatternFields) {
                        var ObjectFields = {};
                        for (var name in PatternFields) {
                            ObjectFields[name] = {};
                            ObjectFields[name]["value"] = objectProperties[name];
                            ObjectFields[name]["type"] = PatternFields[name]["type"];
                            ObjectFields[name]["subtype"] = PatternFields[name]["subtype"];
                            ObjectFields[name]["required"] = PatternFields[name]["required"];
                            ObjectFields[name]["default"] = PatternFields[name]["default"];
                        }
                        properties(Object.oid, "edit", ObjectFields, function (editedProperties) {
                            var fields = {};
                            for (var name in editedProperties) {
                                fields[name] = editedProperties[name].value;
                            }
                            ecomet.edit_object(Object.oid, fields, function () { }, function (ErrorText) {
                                errordlg(ErrorText);
                            });
                            navigatorGrid.refresh();
                        });
                    });
                },
                function (ErrorText) { errordlg(ErrorText); }
            );
        }

        function deleteObject() {
            var selectedArray = navigatorGrid.getSelected();
            for (var i in selectedArray) {
                ecomet.delete_object(selectedArray[i].object.oid, function () { }, function (ErrorText) {
                    errordlg(ErrorText);
                });
            }
            navigatorGrid.refresh();
        }

        function getPatternFields(PatternPath, OnResult) {
            ecomet.find("GET .oid from * WHERE .path='" + PatternPath + "'",
                function (result) {
                    var PatternFolder = result.set.pop().oid;
                    ecomet.find("GET .name,type,subtype,required,default from * WHERE .folder=$oid('" + PatternFolder + "')",
                        function (result) {
                            var PatternFields = {};
                            for (var i in result.set) {
                                PatternFields[result.set[i].fields[".name"]] = {
                                    "type": result.set[i].fields["type"],
                                    "subtype": result.set[i].fields["subtype"],
                                    "required": result.set[i].fields["required"],
                                    "default": result.set[i].fields["default"],
                                }
                            }
                            OnResult(PatternFields);
                        },
                        function (ErrorText) { errordlg(ErrorText); }
                    );
                },
                function (ErrorText) { errordlg(ErrorText); }
            );
        }

        function exportPattern() {
            select_impexp(function (Pattern, Fields) {
                ecomet.application(
                    "ecomet_impexp",
                    "export_pattern",
                    [Pattern, Fields],
                    function (Result) { errordlg(Pattern + " - " + Result); },
                    function (Error) { errordlg(Pattern + " - " + Error); }
                );
            });
        }

        function importPattern() {
            select_impexp(function (Pattern, Fields) {
                ecomet.application(
                    "ecomet_impexp",
                    "import_pattern",
                    [Pattern, Fields],
                    function (Result) { errordlg(Pattern + " - " + Result); },
                    function (Error) { errordlg(Pattern + " - " + Error); }
                );
            });
        }
        //===============================================================
        //  Binding
        //===============================================================
        function draw() {
            loadcss("css/ecomet/main.css");
            $("body").load("markup/main.html", function () {
                //Bind jquery objects
                $btncreate = $('#ctrlbar *[name="create"]');
                $btnedit = $('#ctrlbar *[name="edit"]');
                $btndelete = $('#ctrlbar *[name="delete"]');
                $btnquery = $('#ctrlbar *[name="query"]');
                // Init jquery objects
                $btncreate.button();
                $btncreate.click(createObject);
                $btnedit.button({ disabled: true });
                $btnedit.click(editObject);
                $btndelete.button({ disabled: true });
                $btndelete.click(deleteObject);
                $btnquery.button();
                $btnquery.click(query);
                navigator(
                    $('#navigator'),
                    {
                        "startPath": "/root",
                        "fields": ['.name', '.pattern'],
                        "height": 750,
                        "multiselect": true,
                        "onSelectRow": configButtons,
                        "onChangeFolder": configButtons
                    },
                    function (readyNavigator) {
                        navigatorGrid = readyNavigator;
                    }
                );
            });
        }
    });