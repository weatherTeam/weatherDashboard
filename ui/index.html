<?php
$navigation = array();

if (isset($_GET['page'])) {
    $pageURLName = $_GET['page'];
} else {
    $pageURLName = "home";
}

$pages = array(
    array("Home", "home"),
    array("All stations", "all-stations"),
    array("Snow cumulation", "snow-cumulation"),
    array("Rainfall", "rainfall"),
    array("Nearest neighbour", "nearest-neighbour"),
    array("Wikipedia", "wikipedia"),
    array("Temperature anomalies", "temperature-anomalies"),
    array("Storms", "storms")
    );

    foreach ($pages as $page) {
        if ($page[1] == $pageURLName) {
            $pageName = $page[0];
            $pageContent = $page[1].'.html';
        }
    }

?>
<!DOCTYPE html>
<html>
<head>
    <meta charset=utf-8 />
    <title>Weather Dashboard - <?php echo $pageName; ?></title>
    <link href='https://api.tiles.mapbox.com/mapbox.js/v1.6.2/mapbox.css' rel='stylesheet' />
    <link rel="stylesheet" href="http://cdn.leafletjs.com/leaflet-0.7.2/leaflet.css" />
    <!--<link rel="stylesheet" type="text/css" href="//netdna.bootstrapcdn.com/bootstrap/3.0.3/css/bootstrap.min.css">-->
    <link rel="stylesheet" type="text/css" media="screen" href="style.css" />
    <script src='https://api.tiles.mapbox.com/mapbox.js/v1.6.2/mapbox.js'></script>
    <!-- <script src="http://cdn.leafletjs.com/leaflet-0.7.2/leaflet.js"></script> -->
    <script src="//code.jquery.com/jquery-1.11.0.min.js"></script>
    <script src="//code.jquery.com/jquery-migrate-1.2.1.min.js"></script>
    <script type="text/javascript" language="javascript" src="http://cdn.datatables.net/1.10-dev/js/jquery.dataTables.min.js"></script>
    <script type="text/javascript" src="js/jquery.csvToTable.js"></script>
    <link href='http://fonts.googleapis.com/css?family=Amaranth:400,400italic,700' rel='stylesheet' type='text/css'>
</head>
<body>
<header>
    <h1>Weather Dashboard</h1>
    <nav>
        <?php
            foreach ($pages as $menuitem) {
                if ($menuitem[1] == $pageURLName) {
                    $menuItemClass = 'class="currentPage"';
                } else {
                    $menuItemClass = '';
                }
                ?>
                <menuitem><a href="<?php echo $menuitem[1]; ?>" <?php echo $menuItemClass; ?>><?php echo $menuitem[0]; ?></a></menuitem>
                <?php
            }
        ?>
    </nav>
</header>
<main>
<!--<h2><?php echo $pageName; ?></h2>-->
<?php
//echo "<pre>".print_r($pages)."</pre>";
if (is_file('pages/'.$pageContent)) {
    include('pages/'.$pageContent);
}
?>
</main>
<footer>
As part of the course <em>Big Data</em> of the <a href="http://www.epfl.ch">EPFL</a> given by Christoph Koch<br />
By Aubry Cholleton, Jonathan Duss, Anders Hennum, Alexis Kessel, Quentin Mazars-Simon, Cédric Rolland, Orianne Rollier, David Sandoz & Amato Van Geyt<br />
Under the supervision of Amir Shaikhha
</footer>
</body>
</html>
