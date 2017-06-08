/**
 * Created by J on 4/21/14.
 */

var brandCats = ['fi', 'it', 'gov', 'id', 'hc', 'edu'];
var brandCounts = {};
brandCounts['fi'] = 11;
brandCounts['it'] = 16;
brandCounts['gov'] = 12;
brandCounts['hc'] = 11;
brandCounts['id'] = 12;
brandCounts['edu'] = 6;

function startBrandSlideshow(brandCat, divElementClassName) {
    if(typeof(brandCat) === 'undefined') {
        brandCat = 'all';
    }

    if(typeof(divElementClassName) === 'undefined') {
        divElementClassName = 'brandslideshow';
    }

    if($('div.' + divElementClassName).length == 0) {
        alert('NOT FOUND: $(div.' + divElementClassName + "')");
        return;
    }

    //$('div.' + divElementClassName).cycle();
    $('div.' + divElementClassName).cycle('destroy');

    var count = brandCounts[brandCat];
    var brands = brandCat == 'all' ? getBrandsRandomized(3) : getBrands(brandCat);
    var markup = '';
    for(var b = 0; b < brands.length; b++) {
        markup += '<img src="/res/img/' + brands[b] + '" width="180" height="80" style="display: none;">\n';
    }

    $('div.' + divElementClassName).empty();
    $('div.' + divElementClassName).append(markup);

    $('div.' + divElementClassName).cycle();
    //$('div.' + divElementClassName).cycle('add', markup);
}

/**
 * Returns an array of all brand file names in order for the given category.
 * @param brandCat
 * @returns {Array}
 */
function getBrands(brandCat) {
    var brands = [];
    var counts = brandCounts[brandCat];
    for(var i = 1; i <= counts; i++) {
        brands.push(getBrandFileName(brandCat, i));
    }

    return brands;
}

/** Returns an array of brand image names.
 * chunkSize is used to bias randomization over order (smaller number means more important) */
function getBrandsRandomized(chunkSize) {
    if(typeof(chunkSize) === 'undefined') {
        chunkSize = 3;
    }
    var cats = brandCats;
    var counts = brandCounts;

    var overallArray = [];
    var done = false;
    var rangeStart = 1;

    while(true) {
        var chunkArray = [];
        for(var i = 0; i < cats.length; i++) { // for each cat
            var cat = cats[i];
            var count = counts[cat];

            for(var j = rangeStart; j < rangeStart + chunkSize && j <= count; j++) { // for each element in this chunk
                chunkArray.push(getBrandFileName(cat, j));
            }
        }

        if(chunkArray.length == 0) {
            break;
        }

        shuffle(chunkArray);
        for(var k = 0; k < chunkArray.length; k++) {
            overallArray.push(chunkArray[k]);
        }

        rangeStart += chunkSize;
    }

    return overallArray;
}

function getBrandFileName(cat, num) {
    return 'brand-' + cat + '-' + (num < 10 ? '0' : '') + num + ".png";
}

/**
 * Fisher-Yates Shuffle
 * Ref: http://bost.ocks.org/mike/shuffle/
 * @param array
 * @returns {*}
 */
function shuffle(array) {
    var m = array.length, t, i;

    // While there remain elements to shuffle…
    while (m) {

        // Pick a remaining element…
        i = Math.floor(Math.random() * m--);

        // And swap it with the current element.
        t = array[m];
        array[m] = array[i];
        array[i] = t;
    }

    return array;
}

/** Automatic generation of table of content for resources.php pages */
function autoToc() {
    $(function() {
        if(! ($('#toc').length > 0)) {
            return; // no TOC
        }

        $("div#main h3").each(function(i) {
            var current = $(this);
            current.attr("id", "toc-title-" + i);
            $("#toc").append("<li><a id='link" + i + "' href='#toc-title-" +
                i + "' title='" + current.attr("tagName") + "'>" +
                current.html() + "</a></li>");
        });
    });
}

function emailAddress(host, user) {
    document.write(user + '@' + host);
}

function getJackEmail() {
    emailAddress('hotmail.com', 'jackliguojie');
}

function addFav(_title, _url) {
    if(document.all) {
        window.external.AddFavorite(_url, _title);
    }
}

/////////////////// Email related.
function emailAddress(host, user) {
    document.write(user + '@' + host);
}

function writeEmailAddress(host, user) {
    document.write('<a href="mailto:');
    emailAddress(host, user);
    document.write('">');
    emailAddress(host, user);
    document.write("</a>");
}

function getEmailSupport() {
    writeEmailAddress("asprise.com", "support");
}

function getEmailSales() {
    writeEmailAddress("asprise.com", "sales");
}

function getEmailEnquiry() {
    writeEmailAddress("asprise.com", "info");
}

function getEmailHr() {
    writeEmailAddress("asprise.com", "hr");
}

function writeAddress() {
    var building = "Harbourfront";
    var postCode = "0" + (100 - 2) + (3 + 3) + 33;
    var country = "Singapore";
    var s = "1 " + building + " Place, " + building + " Tower One, " + country + ", " + postCode;

    document.write(s);
}

function writePhoneUS() {
    var phone = "+1";
    phone += " (650) " + "521-" + (100 - 1) + "35";
    document.write(phone);
}



function logToConsole(mesg, isError) {
    if(window.console) {
        var dateStr = new Date().toLocaleTimeString();
        if(isError) {
            if(console.error) {
                console.error(dateStr + " " + mesg);
            } else {
                console.log(dateStr + " " + "ERROR: " + mesg);
            }
        } else {
            if(console.info) {
                console.info(dateStr + " " + mesg);
            } else {
                console.log(dateStr + "" + " INFO: " + mesg);
            }
        }
    } else { // no console
        if(isError) {
            if(window.alert) {
                alert("ERROR: " + mesg);
            }
        }
    }
}

// We do not like to be framed.

if(self != top)
    top.location.replace(self.location);