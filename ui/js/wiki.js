$(document).ready(function() {
  $('#wikiTable').CSVToTable('data/events.csv',
    {
      tableClass: "table table-striped table-bordered",
      separator: "\t",
      headers: ['Title', 'Category', 'Start date', 'End date', 'Location', 'ref']
    });
  /*.bind("loadComplete",function() {
    $('#test table').dataTable({
            "aoColumns": [
                null,
                null,
                {"sType": "date-uk"},
                {"sType": "date-uk"},
                null,
                null
            ],
            "iDisplayLength": 25,
            "paging": true,
            "searching": true,
            "info": true,
            "columnDefs": [
            {
                "targets": [ 5 ],
                "visible": false,
                "searchable": false
            }],
            "order": [[ 5, "desc" ]]
        } );
});*/


$("#period").change(function(event){
  filterWiki('', usa, $('#period').val())
});

$("#period2").change(function(event){
  filterWiki('', usa, $('#period2').val())
});

});


var usa = new RegExp("united states|america|alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|district of columbia|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|wisconsin|wyoming");
var ch = new RegExp("switzerland|europe");




//Filter wikipedia table rows based on parameters
//Based on http://jsfiddle.net/ukW2C/3/
function filterWiki(category, place, period){

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
        var rowStartDate = moment($t.children()[2].innerText, "DD/MM/YYYY");
        var rowEndDate = moment($t.children()[3].innerText, "DD/MM/YYYY").add('ms', 42); //We add a few seconds so that it will be a range even if periodStartDate and periodEndDate are the same.
        var rowRange = moment().range(rowStartDate, rowEndDate);


        if(period.split('-').length === 2){//Month

          periodStartDate = moment(period, "YYYY-MM");
          numDaysMonth = periodStartDate.daysInMonth();
          periodEndDate = moment(period, "YYYY-MM").add('d', numDaysMonth).subtract('ms', 1);
          var periodRange = moment().range(periodStartDate, periodEndDate);

        } else { //Day

          periodStartDate = moment(period, "YYYY-MM-DD");
          periodEndDate = moment(period, "YYYY-MM-DD").add('h', 23);
          var periodRange = moment().range(periodStartDate, periodEndDate);

        }

        dateOk = rowRange.overlaps(periodRange);

        return dateOk;

    }).show(); // Show only necessary rows
  }
