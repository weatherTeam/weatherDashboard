$(document).ready(function() {


$('#wikiTable').CSVToTable('events.csv',
{
  tableClass: "table table-striped table-bordered",
  separator: "\t",
  headers: ['Title', 'Category', 'Start date', 'End date', 'Location', 'ref']
    });


/*$("#period").change(function(event){
  setTimeout(function() {
    filterWiki('', '', $('#period').val(), $('input[name=periodType]:checked').val());
  }, 100);
});

$("#periodType").change(function(event){
  setTimeout(function() {
    filterWiki('', '', $('#period').val(), $('input[name=periodType]:checked').val());
  }, 100);

});*/

});

/*
var usa = new RegExp("united states|america|alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|district of columbia|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|wisconsin|wyoming");
var ch = new RegExp("switzerland|europe");
*/




//Filter wikipedia table rows based on parameters
//Based on http://jsfiddle.net/ukW2C/3/
function filterWiki(category, place, period, periodType){


    then = new Date();

    //Create a jquery object of the rows
    var rows = $("#wikiTable").find("tr");

    //Hide all the rows
    rows.hide();

    //Recusively filter the jquery object to get results.
    rows.filter(function (i, v) {

        if ( i === 0 ) { // if header
          return true;
        }

        var $t = $(this);


        //Filter with category
        var rowCategory = $t.children()[1].innerText;
        categoryOk = category === '' || category===rowCategory;

        if (!categoryOk){
          return false;
        }

        //Filter with location
        var rowPlace = $t.children()[4].innerText;
        placeOk = place === '' || place.test(rowPlace);

        if(!placeOk){
          return false;
        }
        
        //Filter with date
        var splittedStartDate = $t.children()[2].innerText.split('/');
        var rowStartDate = splittedStartDate[2]+'-'+splittedStartDate[1]+'-'+splittedStartDate[0];

        var splittedEndDate = $t.children()[3].innerText.split('/');
        var rowEndDate = splittedEndDate[2]+'-'+splittedEndDate[1]+'-'+splittedEndDate[0];

        if(periodType === 'Month'){//Month

          splittedDate = period.split('-');
          year = splittedDate[0];
          month = splittedDate[1];
          periodStartDate = year+'-'+month+'-01';
          periodEndDate = year+'-'+month+'-31';

        } else if(periodType === 'Day'){ //Day

          periodStartDate = period;
          periodEndDate = period;

        } if(periodType === 'Year'){

          year = period.split('-')[0];
          periodStartDate = year+'-01-01';
          periodEndDate = year+'-12-31';
        }

        //if the event starts after the end of the period, or ends before the end of the period it should be filtered  

        return !( rowStartDate > periodEndDate || rowEndDate < periodStartDate );

    }).show(); // Show only necessary rows

  now = new Date();

  console.log(now-then);

}
