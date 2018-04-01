<?php
error_reporting(E_ALL & ~E_NOTICE);
$bootstrap=array(
	'default'=>array(
		'192.168.1.3',
		'192.168.13.3',
	),
);

$host=$_GET['host'];
$uuid=$_GET['uuid'];
if(!isset($host) || (!isset($bootstrap[$host]) && !isset($bootstrap[$uuid])))
	$host='default';
die(json_encode(isset($bootstrap[$host]) ? $bootstrap[$host] : $bootstrap[$uuid], JSON_PRETTY_PRINT));
?>