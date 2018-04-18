<?php
error_reporting(E_ALL & ~E_NOTICE);
$bootstrap=array(
	'default'=>array(
		'192.168.1.3',
		'192.168.13.3',
	),
	'192.168.1.3'=>array(
		'192.168.13.3',
	),
	'192.168.13.3'=>array(
		'192.168.1.3',
	),
);

error_log(serialize($_GET));
$data=$bootstrap[$_GET['uuid']];
if(!isset($data))
{
	error_log("first isset");
	$data=$bootstrap[$_GET['host']];
}
if(!isset($data))
{
	error_log("second isset");
	$data=$bootstrap['default'];
}
error_log("returning ".serialize($data));
die(json_encode($data, JSON_PRETTY_PRINT));
?>