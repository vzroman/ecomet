
var loadcss;
if (!loadcss) {
    loadcss = {};
}

(function () {
   	var loadedCss={};
   	var loadedJs={};
    if (typeof loadcss!== 'function') {
        loadcss = function (url) {
        	if (loadedCss[url]!=1){
        		var link = document.createElement("link");
			    link.type = "text/css";
			    link.rel = "stylesheet";
			    link.media="screen";
			    link.href = url;
			    document.getElementsByTagName("head")[0].appendChild(link);
			    loadedCss[url]=1;
        	}
        }
    }
}());

if (typeof define == 'function') {
    define([],function(){return loadcss});
}