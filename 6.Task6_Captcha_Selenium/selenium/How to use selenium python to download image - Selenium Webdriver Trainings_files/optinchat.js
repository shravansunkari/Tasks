function createIframe() {
    var talkformContainer = document.getElementById('container-talkform');
    if (!talkformContainer) {
        var ocScript = document.getElementById('oc_script');
        var convId = ocScript.getAttribute('convid');
        var chatURL = 'https://app.optinchat.com/#/' + convId;
        var formContainer = '<div id="container-talkform" style="z-index:5000000000;position: fixed;margin: 0px;overflow: auto;bottom: 0px;right: 0px;width: 370px;height: 100px;display: block;"><div class="wrap-p-talkform" style="display: block; position: relative; height: 100%; width: 100%; top: 0%; left: 0px;">';
        var iframeHTML = '<iframe id="oc_iframe" src=' + chatURL + ' title="" style="z-index: 100001; position: absolute; padding: 0px; margin: 0px; width: 100%; height: 100%; border: none; border-radius: 6px !important;right:0px"></iframe>'
        var formEndingTags = '</div></div>';
        var d1 = document.getElementsByTagName("body")[0];;
        d1.insertAdjacentHTML('beforeend', formContainer + iframeHTML + formEndingTags);
    }
}




function optinchat_injectCss(css) {
    /** @type {Element} */
    var style = document.createElement("style");
    /** @type {string} */
    style.type = "text/css";
    /** @type {string} */
    style.rel = "stylesheet";
    style.media = "all";
    try {
        /** @type {string} */
        style.styleSheet.cssText = css;
    } catch (t) {}
    try {
        /** @type {string} */
        style.innerHTML = css;
    } catch (t) {}
    document.getElementsByTagName("head")[0].appendChild(style);
}


function optinchat_getCss() {
    return '.optinchat-mobile-messenger-active {overflow: hidden;height: 100%;width: 100%;position: fixed;}';
}

optinchat_injectCss(optinchat_getCss());

var oc_isMobile = null;

window.addEventListener('message', function(e) {

    if (e.origin === 'https://app.optinchat.com') {
        var message = e.data;
        if (message.isMobile) {
            oc_isMobile = message.isMobile;
        }
        if (message.needLocation) {
            var receiver = document.getElementById('oc_iframe').contentWindow;
            receiver.postMessage({
                parentURL: window.location.href
            }, '*');
            return;
        }


        var container = document.getElementById('container-talkform');

        if (message.isShowed) {
            if (oc_isMobile) {
                container.style.height = '100%';
                container.style.width = '100%';
                var body = document.getElementsByTagName('body');
                if (body && body.length > 0) {
                    body = body[0];
                    body.classList.add("optinchat-mobile-messenger-active");
                }
                return;
            }
            container.style.height = 'calc(100%)';
            container.style.width = '466px';
            container.style['max-height'] = '700px';
            return;
        } else {
            if (container && oc_isMobile) {
                container.style.height = '100px';
                container.style.width = '86px';
                var body = document.getElementsByTagName('body');
                if (body && body.length > 0) {
                    body = body[0];
                    body.classList.remove("optinchat-mobile-messenger-active");
                }
            } else if (container) {
                container.style.height = '100px';
                container.style.width = '370px;'
                container.style['max-height'] = '700px';
            }
        }
    }

});
var OC_DOM = new function() {
    var IS_READY = false;
    var CALLBACKS = [];
    var SELF = this;

    SELF.ready = function(callback) {
        //check to see if we're already finished
        if (IS_READY === true && typeof callback === 'function') {
            callback();
            return;
        }

        //else, add this callback to the queue
        CALLBACKS.push(callback);
    };
    var addEvent = function(event, obj, func) {
        if (window.addEventListener) {
            obj.addEventListener(event, func, false);
        } else if (document.attachEvent) {
            obj.attachEvent('on' + event, func);
        }
    };
    var doScrollCheck = function() {
        //check to see if the callbacks have been fired already
        if (IS_READY === true) {
            return;
        }

        //now try the scrolling check
        try {
            document.documentElement.doScroll('left');
        } catch (error) {
            setTimeout(doScrollCheck, 1);
            return;
        }

        //there were no errors with the scroll check and the callbacks have not yet fired, so fire them now
        fireCallbacks();
    };
    var fireCallbacks = function() {
        //check to make sure these fallbacks have not been fired already
        if (IS_READY === true) {
            return;
        }

        //loop through the callbacks and fire each one
        var callback = false;
        for (var i = 0, len = CALLBACKS.length; i < len; i++) {
            callback = CALLBACKS[i];
            if (typeof callback === 'function') {
                callback();
            }
        }

        //now set a flag to indicate that callbacks have already been fired
        IS_READY = true;
    };
    var listenForDocumentReady = function() {
        //check the document readystate
        if (document.readyState === 'complete') {
            return fireCallbacks();
        }

        //begin binding events based on the current browser
        if (document.addEventListener) {
            addEvent('DOMContentLoaded', document, fireCallbacks);
            addEvent('load', window, fireCallbacks);
        } else if (document.attachEvent) {
            addEvent('load', window, fireCallbacks);
            addEvent('readystatechange', document, fireCallbacks);

            //check for the scroll stuff
            if (document.documentElement.doScroll && window.frameset === null) {
                doScrollCheck();
            }
        }
    };

    //since we have the function declared, start listening
    listenForDocumentReady();
};

//simple use case
OC_DOM.ready(function() {
    console.log('dom ready called');
    createIframe();
});

// window.firebaseCb = function() {
//
//
// }
//
// optinchat_loadJS('https://www.gstatic.com/firebasejs/3.6.9/firebase.js');



document.addEventListener("DOMContentLoaded", function(event) {
    console.log("DOM fully loaded and parsed");
});
