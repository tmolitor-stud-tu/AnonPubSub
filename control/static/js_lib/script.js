"use strict"; (function() {		//use strict javascript mode and don't pollute global namespace
var min_log_height = 8;
var renderer;
var canvas;
var nodes = [];
var connection_states = {};
var running = false;
var commands_pending = {};

//some utility functions
var downloadString = function(text, fileType, fileName) {
	var blob = new Blob([text], { type: fileType });
	var a = document.createElement('a');
	a.download = fileName;
	a.href = URL.createObjectURL(blob);
	a.dataset.downloadurl = [fileType, a.download, a.href].join(':');
	a.style.display = "none";
	document.body.appendChild(a);
	a.click();
	document.body.removeChild(a);
	setTimeout(function() { URL.revokeObjectURL(a.href); }, 1500);
}
//see https://stackoverflow.com/a/31424853
const reflect = p => p.then(v => ({v, status: "fulfilled" }), e => ({e, status: "rejected" }));

//high level api to send command to node
var send_command = function(host, command, data) {
	var command_promise = Promise.resolve($.ajax({
		type: "POST",
		url: "http://"+host+":9980/command",
		data: JSON.stringify($.extend({}, data || {}, {"_command": command})),
		processData: false,
		contentType: "application/json; charset=utf-8",
		dataType: "text"
	})).then(function(data) {
		data = data.split("\n");
		if(data[0] != "OK")
			throw data
		return data[1];
	}, function(error) {
		throw "ajax error: "+(error.responseText || error.statusText);
	}).then(function(command_id) {
		return new Promise(function(resolve, reject) {
			if(command_id in commands_pending)		//already received corresponding sse event
			{
				if(commands_pending[command_id].type == "resolve")
					resolve("result" in commands_pending[command_id] ? commands_pending[command_id].result : null);
				else
					reject("error" in commands_pending[command_id] ? commands_pending[command_id].error : host+" backend error: unknown error");
				delete commands_pending[command_id];
			}
			else
			{
				//automatically reject after 16 seconds
				window.setTimeout(function() {
					if(command_id in commands_pending)
						commands_pending[command_id].reject(host+" backend error: timeout");
				}, 16000);
				commands_pending[command_id] = {resolve: resolve, reject: reject};
			}
		});
	});
	return command_promise;
};

//dynamically parse html and build settings object
var parse_settings = function() {
	var settings = {};
	$(".grid-settings .global-settings .setting").each(function() {
		var path = $(this).data("setting").split(".");
		var section = settings;
		var value = $(this).val();
		if($(this).attr("type") == "checkbox")
			value = $(this).prop("checked") ? $(this).data("checked-value") : $(this).data("unchecked-value");
		else if($(this).attr("type") == "number")
			value = parseFloat(value);
		while(path.length)
		{
			var entry = path.shift();
			if(path.length == 0)
				section[entry] = value;
			else if(!(entry in section))
				section[entry] = {}
			section = section[entry];
		}
	});
	return settings;
};

//connection coloring and connection management
var update_connection_state = function(state1, ip1, ip2, active_init, port) {
	if(!canvas)		//sanity check
		return;
	if(port==9998)		//TODO: add new dashed connectors for group connections
		return;
	var connection;
	var connections=canvas.getConnections();
	for(let c=0; c<connections.length; c++)
	{
		if((connections[c].sourceId == "node-"+ip1 && connections[c].targetId == "node-"+ip2) ||
		(connections[c].targetId == "node-"+ip1 && connections[c].sourceId == "node-"+ip2))
			connection = connections[c];
	}
	if(!connection && state1 == "disconnected")		//don't add new connections if they are disconnected
		return;
	if(!connection)
	{
		var source_node = $(document.getElementById("node-"+ip1)).data("node");
		var destination_node = $(document.getElementById("node-"+ip2)).data("node");
		if(!active_init)		//not active init means ip1 has to be the target
			[source_node, destination_node] = [destination_node, source_node];
		connection = canvas.connect({source: source_node.data.obj, target: destination_node.data.obj});
		canvas.graph.newEdge(source_node, destination_node, {});	//needed for graph relayout
	}
	var colors = {"disconnected": "black", "connecting": "yellow", "connected": "green", "disconnecting": "red"};
	var color_connection = function(color) {
		$(connection.canvas).removeClass("black-arrow").removeClass("green-arrow").removeClass("yellow-arrow").removeClass("red-arrow");
		$(connection.canvas).addClass(color+"-arrow");
	};
	var get_common_state = function() {
		var state1 = connection_states["node-"+ip1]["node-"+ip2];
		var state2 = connection_states["node-"+ip2]["node-"+ip1];
		var common_state = "disconnected";
		if(running)		//only color edges when we are in started mode
		{
			if(state1 == "connected" && state2 == "connected")
				common_state = "connected";
			else if(state1 == "connecting" || state2 == "connecting")
				common_state = "connecting";
			else if(state1 == "disconnected" || state2 == "disconnected")
				common_state = "disconnected";
		}
		return common_state;
	};
	
	//init state data structures
	if(typeof(connection_states["node-"+ip1]) == "undefined")
		connection_states["node-"+ip1]={};
	if(typeof(connection_states["node-"+ip2]) == "undefined")
		connection_states["node-"+ip2]={};
	if(typeof(connection_states["node-"+ip1]["node-"+ip2]) == "undefined")
		connection_states["node-"+ip1]["node-"+ip2] = "disconnected";
	if(typeof(connection_states["node-"+ip2]["node-"+ip1]) == "undefined")
		connection_states["node-"+ip1]["node-"+ip2] = "disconnected";
	connection_states["node-"+ip1]["node-"+ip2] = state1;		//save new state
	
	//apply colors
	if(state1 == "disconnected" && running)		//only if we are in started mode
	{
		//edge is in "disconnecting" state for 1 second before dropping it entirely
		color_connection(colors["disconnecting"]);
		window.setTimeout(function() {
			if(get_common_state() == "disconnected")
			{
				var source_node = $(connection.source).data("node");
				var destination_node = $(connection.target).data("node");
				if(!source_node || !destination_node)
					return;
				var edges = canvas.graph.getEdges(source_node, destination_node);
				if(edges.length != 1)		//there will be only one(!!) edge between two nodes at any time (or no edge at all)
					return;
				canvas.detach(connection);
				canvas.graph.removeEdge(edges[0]);
			}
		}, 1000);
		return;		//don't color edge yet
	}
	color_connection(colors[get_common_state()]);
};

//led coloring
var update_led = function(node, led, color, title="") {
	if(!node.data.obj.find(".overlay:visible").length)
	{
		node.data.settings.find(".info .leds .value .led-"+led).css("background-color", color).prop("title", title);
		node.data.obj.find(".leds .led-"+led).css("background-color", color);
	}
	else
	{
		node.data.settings.find(".info .leds .value .led-"+led).css("background-color", "");
		node.data.obj.find(".leds .led-"+led).css("background-color", "");
	}
};

//logging utility function used by various events
var append_to_log = function(node, msg) {
	var log = node.data.log.find(".log");
	var cleanup_old_entries = node.data.log.find(".autocleanup").prop("checked");
	var scroll_down = log[0].scrollHeight - log[0].clientHeight <= log[0].scrollTop + 1;
	window.requestAnimationFrame(function() {
		log.append(msg);
		if(scroll_down)
		{
			//remove old elements in autoscroll mode
			if(cleanup_old_entries)
			{
				var lines = node.data.log.find(".log span");
				node.data.log.find(".log span").slice(0, -128).remove();
			}
			log[0].scrollTop = log[0].scrollHeight - log[0].clientHeight;
		}
	});
};
var str_pad = function(pad, str, padLeft) {
	if(typeof str === 'undefined')
		return pad;
	if(padLeft)
		return (pad + str).slice(-pad.length);
	else
		return (str + pad).substring(0, pad.length);
}

//overlay creation high level api
var create_overlay = function(data) {
	if(!data.nodes || !data.links)
		throw "Unknown file format: No nodes or links found!";
	var graph = new Springy.Graph();
	//wolfram alpha solution for "exponential fit {{5, 800}, {25, 100}, {10, 400}, {15, 200},{30,20}}"
	var layout = new Springy.Layout.ForceDirected(graph, 200.0, 1542.81 * Math.exp(-0.132625 * data.nodes.length), 0.5, 0.25);
	//only change bounding box while rendering and use a fixed one when drag/drop is enabled
	//this prevents node from jumping when the window or log area is resized
	var currentBB = {bottomleft: new Springy.Vector(-2, -2), topright: new Springy.Vector(2, 2)};//layout.getBoundingBox();
	
	// convert to/from screen coordinates
	var get_canvas_dimensions = function() {
		var width = $("#canvas").width();
		var height = $("#canvas").height();
		//return dimensions with margins on the bottom and right corner  (~8em and ~2.5em)
		return {width: width - (8*16), height: height - (2.5*16)};
	};
	var toScreen = function(p) {
		var size = currentBB.topright.subtract(currentBB.bottomleft);
		var sx = p.subtract(currentBB.bottomleft).divide(size.x).x * get_canvas_dimensions().width;
		var sy = p.subtract(currentBB.bottomleft).divide(size.y).y * get_canvas_dimensions().height;
		return new Springy.Vector(sx, sy);
	};
	var fromScreen = function(s) {
		var size = currentBB.topright.subtract(currentBB.bottomleft);
		var px = (s.x / get_canvas_dimensions().width) * size.x + currentBB.bottomleft.x;
		var py = (s.y / get_canvas_dimensions().height) * size.y + currentBB.bottomleft.y;
		return new Springy.Vector(px, py);
	};
	
	var init_sse = function(node, callback) {
		var host = node.data.ip;
		//this is needed because firefox doesn't always reconnect automatically (see https://stackoverflow.com/q/32079582/3528174 for more info)
		var error_handler = function(event) {
			switch(event.target.readyState) {
				case EventSource.CONNECTING:
					break;
				case EventSource.CLOSED:
					//init sse and reattach all event handlers
					if(!node.data.sse_stopped)
						window.setTimeout(function() {
							node.data.sse = new EventSource("http://"+host+":9980/events");
							node.data.sse.onerror = error_handler;
							node.data.sse_stopped = false;
							callback(node);
						}, 500);
					break;
			}
		};
		//init sse
		node.data.sse = new EventSource("http://"+host+":9980/events");
		node.data.sse.onerror = error_handler;
		node.data.sse_stopped = false;
		//attach all event handlers
		callback(node);
	};
	var init_node_div = function(obj) {
		canvas.draggable(obj, {containment: "parent", grid: [8, 8], stop: function(event, ui) {
			var node = $(event.el).data("node");
			var canvas_offset = $("#canvas").offset();
			var node_offset = node.data.obj.offset();
			var point = layout.point(node);
			var p = fromScreen(new Springy.Vector(node_offset.left - canvas_offset.left, node_offset.top - canvas_offset.top));
			point.p.x = p.x;
			point.p.y = p.y;
		}});
		canvas.makeTarget(obj, {
			dropOptions: { hoverClass: "dragHover" },
			anchor: "Continuous"
		});
		canvas.makeSource(obj, {
			filter: ".ep",
			anchor: "Continuous",
			connector: [ "StateMachine", { curviness: 1 } ],
			connectorStyle: { strokeStyle: "rgb(0,0,0)", lineWidth: 2 },
			maxConnections: 25,
			onMaxConnections: function(info, e) {
				alert("You have reached the maximum number of connections per node (" + info.maxConnections + ").");
			}
		});
		obj.click(function() {
			var node = $(this).data("node");
			$(document).trigger("aps.userinput.node_clicked", node);
		});
	};
	
	//initialize graph canvas (jsplumb instance)
	canvas = jsPlumb.getInstance({
		PaintStyle: { 
			lineWidth: 6, 
			outlineColor: "black", 
			outlineWidth: 1,
		},
		Endpoint: ["Dot", {radius: 3}],
		EndpointStyle: {fillStyle: "#567567"},
		HoverPaintStyle: {strokeStyle: "#1e8151", lineWidth: 2 },
		ConnectionOverlays: [
			["Arrow", { 
				location: 1,
				length: 14,
				foldback: 0.8
			}]
		],
		allowLoopback: false,
	});
	canvas.graph = graph;
	canvas.setContainer($("#canvas"));
	canvas.bind("click", function(connection) {
		$(document).trigger("aps.userinput.disconnect", [$(connection.source).data("node"), $(connection.target).data("node"), connection]);
	});
	canvas.bind("beforeDrop", function(info, event) {
		var connections=canvas.getConnections();
		var map={};
		var add_to_map = function(source, target) {
			if(typeof(map[source]) == "undefined")
				map[source]=[];
			map[source].push(target);
		};
		if(info.sourceId == info.targetId)
		{
			alert("Nodes cannot connect to themselves!");
			return false;	//disallow connection drop
		}
		//build a map of all connections
		for(let c=0; c<connections.length; c++)
			add_to_map(connections[c].sourceId, connections[c].targetId);
		//check if connection is already present (connections are UNdirected here!)
		if($.inArray(info.targetId, map[info.sourceId]) != -1 || $.inArray(info.sourceId, map[info.targetId]) != -1)
		{
			alert("Two nodes can only be connected once!");
			return false;	//disallow connection drop
		}
		var source_node = $(document.getElementById(info.sourceId)).data("node");
		var destination_node = $(document.getElementById(info.targetId)).data("node");
		$(document).trigger("aps.userinput.connect", [source_node, destination_node]);
		return false;		//dont allow connection drop (will be "dropped" programmatically)
	});
	
	//cleanup old dom elements and graph nodes
	$("#canvas").empty().hide();		//remove old elements and hide canvas until all elements are added
	//remove all logcontainers and show dummy logcontainer
	$(".right-log .logcontainer:not(.node-none)").remove();
	$(".right-log .logcontainer.node-none").css("display", "grid");
	//remove all node-settings and display global settings instead
	$(".grid-settings .node-settings").remove();
	$(".grid-settings .global-settings").show();
	//stop all nodes and disconnect their sse streams
	$.each(nodes, function(_, node) {
		send_command(node.data.ip, "stop");
		node.data.sse_closed = true;
		node.data.sse.close();
	});
	nodes = [];
	connection_states = {};
	
	//(re)fill graph canvas with nodes and connections
	canvas.batch(function () {
		var map={};		//this map is used to detect duplicate edges when reading the initial graph (a sanity check)
		var add_to_map = function(source, target) {
			if(typeof(map[source]) == "undefined")
				map[source]=[];
			map[source].push(target);
		};
		$.each(data.nodes, function(_, node) {
			node.ip = node.ip || node.id;		//use id as ip if no ip is given
			//sanity checks
			if(!node.roles)
				node.roles = {}
			if(!node.roles["publisher"])
				node.roles["publisher"] = [];
			if(!node.roles["subscriber"])
				node.roles["subscriber"] = [];
			if(document.getElementById("node-"+node.ip))
			{
				alert("Node '"+node.id+"' already present, ignoring second entry!");
				return true;
			}
			var log = $(".logcontainer-template").clone().removeClass("logcontainer-template").attr("id", "log-"+node.ip);
			var settings = $(".node-settings-template").clone().removeClass("node-settings-template").attr("id", "settings-"+node.ip);
			var obj = $(".node-template").clone().removeClass("node-template").attr("id", "node-"+node.ip);
			$.each(node.roles["publisher"], function(_, channel) {
				var entry = $(".pub-item-template").clone().removeClass("pub-item-template");
				entry.find(".channel").text(channel);
				settings.find(".actions .pub-list").append(entry);
			});
			$.each(node.roles["subscriber"], function(_, channel) {
				var entry = $(".sub-item-template").clone().removeClass("sub-item-template");
				entry.find(".channel").text(channel);
				settings.find(".actions .sub-list").append(entry);
			});
			obj.find("label").append(node.id);
			obj.css({top: 0, left: 0});
			$(".right-log").append(log);
			$(".grid-settings").append(settings);
			$("#canvas").append(obj);
			init_node_div(obj);
			node = graph.newNode($.extend({}, node, {label: node.id, ip: node.ip, obj: obj, log: log, settings: settings}));
			log.data("node", node);
			obj.data("node", node);
			settings.data("node", node);
			nodes.push(node);
		});
		$.each(data.links, function(_, edge) {
			if(!(edge["source"] in nodes))		//sanity check
			{
				alert("Node entry '"+edge["source"]+"' not present in nodes list, ignoring connection referencing this node!");
				return true;
			}
			if(!(edge["target"] in nodes))		//sanity check
			{
				alert("Node entry '"+edge["target"]+"' not present in nodes list, ignoring connection referencing this node!");
				return true;
			}
			//only allow one connection between two nodes
			if($.inArray(edge["target"], map[edge["source"]]) != -1 || $.inArray(edge["source"], map[edge["target"]]) != -1)
			{
				alert("Connection between '"+nodes[edge["source"]].data.label+"' and '"+nodes[edge["target"]].data.label+"' already present, ignoring additional one!");
				return true;
			}
			canvas.connect({source: nodes[edge["source"]].data.obj, target: nodes[edge["target"]].data.obj});
			graph.newEdge(nodes[edge["source"]], nodes[edge["target"]], {});
			add_to_map(edge["source"], edge["target"]);		//update sanity check map
		});
	});
	$("#canvas").show();		//make canvas visible again
	
	//layout graph
	(function() {
		var canvas_offset = $("#canvas").offset();
		var position_node = function(node, p) {
			var pos = toScreen(p);
			node.data.obj.offset({top: canvas_offset.top + pos.y, left: canvas_offset.left + pos.x});
			canvas.revalidate(node.data.obj);	//update connectors, endpoints etc.
		};
		renderer = new Springy.Renderer(layout,
			function clear() {
				//nothing to do here
			},
			function drawEdge(edge, p1, p2) {
				//nothing to do here
			},
			function drawNode(node, p) {
				//position a node
				position_node(node, p);
				//only change bounding box while rendering and use a fixed one when drag/drop is enabled
				//this prevents node from jumping slightly when the window or log area is resized
				currentBB = layout.getBoundingBox();
			}
		);
		renderer.graphChanged = $.noop;		//don't automatically relayout
		//recalculate node positions on canvas resize
		$(window).resize(function() {
			window.requestAnimationFrame(function() {
				$.each(nodes, function(_, node) {
					var point = layout.point(node);
					position_node(node, point.p);
				});
			});
		});
		//start actual rendering (done only once when the json graph file is loaded)
		renderer.start();
		window.setTimeout(function() {
			renderer.stop();
		}, 15000);
	})();
	
	//stop each node and initializing sse
	var first_node = false;
	$.each(nodes, function(_, node) {
		send_command(node.data.ip, "stop");		//make sure the node is stopped now
		init_sse(node, function(node) {
			//hide log placeholder and show first "real" log window
			if(!first_node)
			{
				first_node = true;
				$(".right-log .logcontainer.node-none").css("display", "none");
				node.data.log.css("display", "grid");
			}
			//initialize uuid and ip in log window
			node.data.log.find(".toolbar .node .uuid").text("");
			node.data.log.find(".toolbar .node .host").text(node.data.ip);
			//initialize label, uuid and ip in settings window
			node.data.settings.find(".info .label .value").text(node.data.id);
			node.data.settings.find(".info .uuid .value").html("<i>offline</i>");
			node.data.settings.find(".info .host .value").text(node.data.ip);
			
			//bind to message event and trigger corresponding high level api events
			node.data.sse.addEventListener("message", function(e) {
				var data = JSON.parse(e.data);
				//handle command framework events internally only
				var handle = function(type) {
					if(!(data.data.id in commands_pending))
					{
						if(type == "resolve")
							commands_pending[data.data.id] = {type: type, result: data.data.result || null};
						else
							commands_pending[data.data.id] = {type: type, error: data.data.error || null};
						return;
					}
					if(type == "resolve")
						commands_pending[data.data.id].resolve(data.data.result || null);
					else
						commands_pending[data.data.id].reject(data.data.error ? node.data.ip+": "+data.data.error : null);
					delete commands_pending[data.data.id];
				};
				switch(data.event)
				{
					case "command_completed": handle("resolve"); return;
					case "command_failed": handle("reject"); return;
				}
				//trigger high level api events
				$(document).trigger("aps.backend."+data.event, [node, data.data, e]);
			}, false);
			//bind to basic sse events and trigger corresponding high level api events
			$.each(["open", "error"], function(_, event_name) {
				node.data.sse.addEventListener(event_name, function(e) {
					$(document).trigger("aps.backend."+event_name, [node, {}, e]);
				}, false);
			});
		});
	});
};

window.less.pageLoadFinished.then(function() { $(document).ready(function() {	//wait for lesscss rendering AND dom load
	//header window
	var load_filters = function(silence_errors) {
		var file = $("#filterFileInput").get(0).files[0];
		if(!file)
			return;
		var reader = new FileReader();
		reader.readAsText(file);
		$(reader).load(function() {
			var promises = [];
			var error_seen = false;		//all errors are identical --> show only one of them
			$.each(nodes, function(_, node) {
				promises.push(send_command(node.data.ip, "load_filters", {"code": reader.result}).catch(function(err) {
					if(!error_seen && (typeof(silence_errors)=="undefined" || !silence_errors))
						alert(err);
					error_seen = true;
				}));
			});
			Promise.all(promises.map(reflect)).then(function() {
				$("#load-filters span").hide();
				if(error_seen)
				{
					$("#load-filters .crossmark").css("display", "inline-block");
					$("#load-filters").prop("title", "Filters NOT loaded due to errors");
				}
				else
				{
					$("#load-filters .checkmark").css("display", "inline-block");
					$("#load-filters").prop("title", "Filters loaded successfully");
				}
			});
		});
	};
	var load_graph = function() {
		var file = $("#graphFileInput").get(0).files[0];
		if(!file)
			return;
		var set_error_status = function() {
			$(".grid-status .status").text("Error loading graph file...");
			window.setTimeout(function() { $(".grid-status .status").text("IDLE"); }, 2000);
		};
		$("#stop").click();
		$("#start").prop("disabled", true);
		$("#stop").prop("disabled", true);
		$("#dump-all").prop("disabled", true);
		$(".grid-status .status").text("Loading graph file...");
		var reader = new FileReader();
		reader.readAsText(file);
		$(reader).load(function() {
			var data;
			try {
				data = JSON.parse(reader.result);
				//import settings if given and the import is wanted
				if("graph" in data && "settings" in data.graph && window.confirm("Found settings in graph file, import these, too?"))
				{
					var set_value = function(path, value) {
						$(".grid-settings .global-settings .setting").each(function() {
							if(path == $(this).data("setting"))
							{
								if($(this).attr("type") == "checkbox")
									$(this).prop("checked", value == $(this).data("checked-value") ? true : false);
								else
									$(this).val(value);
							}
						});
					};
					var traverse_settings = function(path, data) {
						$.each(data, function(key, value) {
							path.push(key);
							if($.isPlainObject(value))
								traverse_settings(path, value);
							else
								set_value(path.join("."), value);
						});
					};
					traverse_settings([], data.graph["settings"]);
				}
				//parse graph data
				create_overlay(data);
				//enable start and graph save button
				$("#start").prop("disabled", false);
				$("#save").prop("disabled", false);
				$(".grid-status .status").text("IDLE");
				load_filters(true);		// (re)load filters (could be lost after resetting nodes)
			} catch(e) {
				set_error_status();
				alert("Error loading file: "+e);
			}
		}).error(function() {
			set_error_status();
			alert("Could not load file!");
		});
	};
	$("#graphFileInput").change(load_graph);
	$("#load").click(function() {
		$("#graphFileInput").click();
	});
	$("#save").click(function() {
		if(!$("#canvas .node").length)
		{
			alert("Please load a graph first!");
			return;
		}
		var graph_nodes = [];
		var graph_links = [];
		var id_index_map = {};
		$.each(nodes, function(_, node) {
			var filtered = {"id": node.data.id, "roles": node.data.roles};
			if(node.data.ip != node.data.id)		//add explicit ip only if ip != id
				filtered["ip"] = node.data.ip;
			id_index_map[node.data.id] = graph_nodes.length;
			graph_nodes.push(filtered);
		});
		$.each(canvas.getConnections(), function(_, connection) {
			var source_node = $(document.getElementById(connection.sourceId)).data("node");
			var destination_node = $(document.getElementById(connection.targetId)).data("node");
			graph_links.push({"source": id_index_map[source_node.data.id], "target": id_index_map[destination_node.data.id]});
		});
		var graph = {
			"directed": false,
			"multigraph": false,
			"nodes": graph_nodes,
			"links": graph_links,
			"graph": {
				"name": "WebUI Graph ("+graph_nodes.length+", "+graph_links.length+")",
				"settings": parse_settings()
			}
		};
		downloadString(JSON.stringify(graph, null, 2), "text/json", "graph.json");
	});
	$("#filterFileInput").change(function() { load_filters(); });
	$("#load-filters").click(function() {
		$("#filterFileInput").click();
	});
	$("#start").click(function() {
		if(!$("#canvas .node").length)
		{
			alert("Please load a graph first!");
			return;
		}
		
		var settings = parse_settings();
		$(this).prop("disabled", true);
		$("#stop").prop("disabled", false);
		$("#dump-all").prop("disabled", false);
		
		//indicate we are running now
		running = true;
		//remove all edges (they will be dynamically added back by backend events later)
		var connections = canvas.getConnections();
		$.each(canvas.graph.edges.slice(), function(_, edge) {		//use slice() to copy array (changing while iterating is no good idea)
			canvas.graph.removeEdge(edge);
		});
		$.each(connections, function(_, connection) {
			canvas.detach(connection);
		});
		//start nodes
		$(".grid-status .status").text("Starting nodes...");
		var promises = [];
		$.each(nodes, function(_, node) {
			promises.push(send_command(node.data.ip, "start", {
				"router": $("#router").val(),
				"settings": settings
			}).catch(function(error) {
				alert("Could not start router: "+error);
			}).then(function() {
				//disable node-setting start button
				node.data.settings.find(".actions .start").prop("disabled", true);
			}));
		});
		//let startup complete before trying to connect nodes
		Promise.all(promises.map(reflect)).then(function() {
			$(".grid-status .status").text("Connecting nodes...");
			load_filters(true);		// (re)load filters (could be lost after resetting nodes)
			promises = [];
			$.each(connections, function(_, connection) {
				var source_node = $(document.getElementById(connection.sourceId)).data("node");
				var destination_node = $(document.getElementById(connection.targetId)).data("node");
				promises.push(send_command(source_node.data.ip, "connect", {
					"addr": destination_node.data.ip
				}));
			});
		});
		//wait for all connection commands to be finished and assign roles after waiting
		//an additional 2 seconds to make sure everything settled down properly
		Promise.all(promises.map(reflect)).then(function() {
			$(".grid-status .status").text("IDLE");
			window.setTimeout(function() {
				$(".grid-status .status").text("Assigning roles...");
				promises = [];
				$.each(nodes, function(_, node) {
					var roles = node.data.roles;
					$.each(roles, function(type, channels) {
						if(type == "subscriber")
							$.each(channels, function(_, channel) {
								promises.push(send_command(node.data.ip, "subscribe", {
									"channel": channel
								}));
							});
						else if(type == "publisher")
							$.each(channels, function(_, channel) {
								promises.push(send_command(node.data.ip, "publish", {
									"channel": channel
								}));
							});
						else
							alert("Role '"+type+"' for node '"+node.data.label+"' unknown, ignoring it!");
					});
				});
				Promise.all(promises.map(reflect)).then(function() {
					$(".grid-status .status").text("IDLE");
				});
			}, 2000);
		});
	});
	$("#stop").click(function() {
		running = false;
		$(this).prop("disabled", true);
		$(".grid-status .status").text("Stopping routers...");
		var promises = [];
		$.each(nodes, function(_, node) {
			promises.push(send_command(node.data.ip, "stop").catch(function(error) {
				alert("Could not stop router: "+error);
			}));
			//diable node-setting start button
			node.data.settings.find(".actions .start").prop("disabled", true);
		});
		Promise.all(promises.map(reflect)).then(function() {
			$("#start").prop("disabled", false);
			$(".grid-status .status").text("IDLE");
		});
	});
	$("#dump-all").click(function() {
		$.each(nodes, function(_, node) {
			send_command(node.data.ip, "dump");
		});
	});
	$("#relayout").click(function() {
		renderer.start();
		window.setTimeout(function() {
			renderer.stop();
		}, 15000);
	});
	$("#reset").click(function() {
		if(confirm("Do you really want to reset all nodes?"))
		{
			running = false;
			//stop all nodes
			$(".grid-status .status").text("Stopping routers...");
			var promises = [];
			$.each(nodes, function(_, node) {
				//diable node-setting start button
				node.data.settings.find(".actions .start").prop("disabled", true);
				promises.push(send_command(node.data.ip, "stop"));
			});
			Promise.all(promises.map(reflect)).then(function() {
				//disable stop button and enable start button
				$("#start").prop("disabled", false);
				$("#stop").prop("disabled", true);
				
				$(".grid-status .status").text("Resetting nodes...");
				promises = [];
				$.each(nodes, function(_, node) {
					promises.push(send_command(node.data.ip, "reset").catch(function(error) {
						alert("Could not reset node: "+error);
					}));
				});
				Promise.all(promises.map(reflect)).then(function() {
					//wait some more time
					window.setTimeout(function() {
						$(".grid-status .status").text("IDLE");
					}, 2000);
				});
			});
		}
	});
	$("#clear-all").click(function() {
		$(".right-log .log span").remove();
	});
	$("#autocleanup-all").change(function() {
		$(".autocleanup").prop("checked", $(this).prop("checked"));
	});
	window.setTimeout(load_graph, 0);
	
	
	//settings window
	$("#router").change(function() {
		$(".router-settings").hide();
		$(".router-settings.router-"+$(this).val()).show();
	}).trigger("change");		//trigger first change on page load
	
	
	//node settings window
	$(document).on("click", ".node-settings .settings-switcher", function() {
		$(".grid-settings .node-settings").hide();
		$(".grid-settings .global-settings").show();
	});
	$(document).on("click", ".node-settings .actions .start", function() {
		var node = $(this).closest(".node-settings").data("node");
		//start node
		send_command(node.data.ip, "start", {
			"router": $("#router").val(),
			"settings": parse_settings()
		}).finally(function() {
			//disable node-settings start button
			node.data.settings.find(".actions .start").prop("disabled", true);
			
			//assign roles after waiting some additional 2 seconds to make sure everything settled down properly
			window.setTimeout(function() {
				var roles = node.data.roles;
				$.each(roles, function(type, channels) {
					if(type == "subscriber")
						$.each(channels, function(_, channel) {
							send_command(node.data.ip, "subscribe", {
								"channel": channel
							});
						});
					else if(type == "publisher")
						$.each(channels, function(_, channel) {
							send_command(node.data.ip, "publish", {
								"channel": channel
							});
						});
				});
			}, 2000);
		});
	});
	$(document).on("click", ".node-settings .actions .stop", function() {
		var node = $(this).closest(".node-settings").data("node");
		send_command(node.data.ip, "stop").finally(function() {
			node.data.settings.find(".actions .start").prop("disabled", false);
		});
	});
	$(document).on("click", ".node-settings .publish", function() {
		var node = $(this).closest(".node-settings").data("node");
		var channel = $(this).parents("table").find(".channel").val();
		if($.inArray(channel, node.data.roles["publisher"]) != -1)
		{
			alert("Channel already published on this node!");
			return;
		}
		if(!channel.length)
			alert("Channel name can not be of zero length!");
		else
		{
			var commit = function() {
				var entry = $(".pub-item-template").clone().removeClass("pub-item-template");
				entry.find(".channel").text(channel);
				node.data.roles["publisher"].push(channel);
				node.data.settings.find(".actions .pub-list").append(entry);
			};
			if(!running || !node.data.settings.find(".actions .start").prop("disabled"))
				commit();
			else
				send_command(node.data.ip, "publish", {channel: channel}).then(commit);
		}
	});
	$(document).on("click", ".node-settings .subscribe", function() {
		var node = $(this).closest(".node-settings").data("node");
		var channel = $(this).parents("table").find(".channel").val();
		if($.inArray(channel, node.data.roles["subscriber"]) != -1)
		{
			alert("Channel already subscribed on this node!");
			return;
		}
		if(!channel.length)
			alert("Channel name can not be of zero length!");
		else
		{
			var commit = function() {
				var entry = $(".sub-item-template").clone().removeClass("sub-item-template");
				entry.find(".channel").text(channel);
				node.data.roles["subscriber"].push(channel);
				node.data.settings.find(".actions .sub-list").append(entry);
			};
			if(!running || !node.data.settings.find(".actions .start").prop("disabled"))
				commit();
			else
				send_command(node.data.ip, "subscribe", {channel: channel}).then(commit);
		}
	});
	$(document).on("click", ".node-settings .actions .pub-list .X", function() {
		var node = $(this).closest(".node-settings").data("node");
		var item = $(this).closest(".pub-item");
		var channel = item.find(".channel").text();
		var commit = function() {
			node.data.roles["publisher"] = node.data.roles["publisher"].filter(item => item !== channel)
			item.remove();
		};
		if(!running || !node.data.settings.find(".actions .start").prop("disabled"))
			commit();
		else
			send_command(node.data.ip, "unpublish", {channel: channel}).then(commit, function(error) {
				alert("Could not unpublish channel '"+channel+"': "+error);
			});
	});
	$(document).on("click", ".node-settings .actions .sub-list .X", function() {
		var node = $(this).closest(".node-settings").data("node");
		var item = $(this).closest(".sub-item");
		var channel = item.find(".channel").text();
		var commit = function() {
			node.data.roles["subscriber"] = node.data.roles["subscriber"].filter(item => item !== channel)
			item.remove();
		}
		if(!running || !node.data.settings.find(".actions .start").prop("disabled"))
			commit();
		else
			send_command(node.data.ip, "unsubscribe", {channel: channel}).then(commit, function(error) {
				alert("Could not unpublish channel '"+channel+"': "+error);
			});
	});
	$(document).on("click", ".node-settings .dump", function() {
		var node = $(this).closest(".node-settings").data("node");
		send_command(node.data.ip, "dump");
	});
	$(document).on("click", ".node-settings .actions .create-group", function() {
		var node = $(this).closest(".node-settings").data("node");
		var channel = window.prompt("What channel to create covergroup for?","test");
		if(channel==null)
			return;
		var ips = window.prompt("List of IPs to connect to (comma separated):","127.0.0.22,127.0.0.30,127.0.0.20");
		if(ips==null)
			return;
		var interval = window.prompt("Message interval (should be the smallest publish interval of all publishers in group:","0.5");
		if(interval==null)
			return;
		send_command(node.data.ip, "create_group", {
			channel: channel,
			ips: ips.split(','),
			interval: parseFloat(interval),
		});
	});
	
	
	//log window
	var in_logwindow_resize = false;
	$(window).resize(function() {
		window.requestAnimationFrame(function() {
			//adjust height of container
			$(".grid-right").height($(window).height() - $(".grid-header").height());
			//adjust height of log container
			$(".right-log").height(Math.max(min_log_height, $(".grid-right").height()-$("#canvas").height()));
			//readjust height of canvas according to our changes above
			$("#canvas").height($(".grid-right").height() - $(".right-log").height());
			if(!in_logwindow_resize)		//needed to prevent moving log container beyond window bottom on window resizes
				$(".right-log").css("top", "");
		});
	});
	$(".right-log").resizable({
		autoHide: false,
		handles: "n",
		minHeight: min_log_height,
		start: function(event, ui) {
			in_logwindow_resize = true;
		}, resize: function(event, ui) {
			window.requestAnimationFrame(function() {
				$("#canvas").height($(".right-log").offset().top - $("#canvas").offset().top);
			});
		}, stop: function(event, ui) {
			in_logwindow_resize = false;
			$(window).trigger("resize");
		}
	});
	$(document).on("click", ".right-log .toolbar .clear", function() {
		$(this).parent().parent().find(".log span").remove();
	});
	$(document).on("change", ".right-log .toolbar .levelname", function() {
		var level = $(this).val();
		var log = $(this).parent().parent().parent().find(".log");
		log.removeClass("DEBUG INFO WARNING ERROR");
		log.addClass(level);
	});
	//allow permanent marking of (multiple) log lines
	$(document).on("click", ".right-log .log span", function() {
		$(this).toggleClass("marked");
	});
	
	
	//high level api events (userinput)
	$(document).on("aps.userinput.connect", function(event, source_node, destination_node) {
		if(running)		//routers already started
			send_command(source_node.data.ip, "connect", {
				"addr": destination_node.data.ip
			});
		else
		{
			canvas.connect({source: source_node.data.obj, target: destination_node.data.obj});
			canvas.graph.newEdge(source_node, destination_node, {});	//needed for graph relayout
		}
	});
	$(document).on("aps.userinput.disconnect", function(event, source_node, destination_node, connection) {
		if(running)		//routers already started
		{
			send_command(source_node.data.ip, "disconnect", {
				"addr": destination_node.data.ip
			});
		}
		else
			canvas.detach(connection);
	});
	var current_node;
	$(document).on("aps.userinput.node_clicked", function(event, node) {
		//save scroll state of current log window
		if(current_node)
		{
			var log = current_node.data.log.find(".log");
			current_node.data.log.data("scroll_state", log[0].scrollHeight - log[0].clientHeight <= log[0].scrollTop + 1);
		}
		//switch to new log window
		$(".right-log .logcontainer").css("display", "none");		//hide all log windows
		node.data.log.css("display", "grid");						//show only this one
		current_node = node;
		//restore scroll state of new log window or scroll to bottom on first view
		var log = node.data.log.find(".log");
		var scroll_state = node.data.log.data("scroll_state");
		if(typeof(scroll_state) == "undefined")
			log[0].scrollTop = log[0].scrollHeight - log[0].clientHeight;		//first view
		else if(scroll_state)
			log[0].scrollTop = log[0].scrollHeight - log[0].clientHeight;		//old state was bottom
		//switch to node info window
		$(".grid-settings .global-settings").hide();
		$(".grid-settings .node-settings").hide();
		node.data.settings.show();
	});
	
	
	//high level api events (backend events)
	$(document).on("aps.backend.open", function(event, node, data) {
		append_to_log(node, $("<span>").addClass("ok").text((new Date().toLocaleString())+" SSE connection to '"+node.data.ip+"' established!").append($("<br>")));
		node.data.obj.find(".overlay").hide();
		node.data.settings.find(".actions .stop").click();
		load_filters(true);		// (re)load filters (could be lost after resetting nodes)
	});
	$(document).on("aps.backend.error", function(event, node, data) {
		append_to_log(node, $("<span>").addClass("error").text((new Date().toLocaleString())+" SSE connection to '"+node.data.ip+"' failed, retrying!").append($("<br>")));
		node.data.obj.find(".overlay").show();		//indicate visibly that our node seems to be offline
		node.data.settings.find(".info .uuid .value").html("<i>offline</i>");
		node.data.obj.prop("title", "offline");
		//clear all leds
		for(let led=0; led<=9; led++)
			update_led(node, led, "");
	});
	$(document).on("aps.backend.log", function(event, node, d) {	//use d instead of data for better readability
		var add_attributes = function(msg) {
			msg.attr("data-host", node.data.ip);
			msg.attr("data-levelname", d.levelname);
			msg.attr("data-module", d.module);
			msg.attr("data-name", d.name);
			msg.attr("data-threadname", (d.threadName || "").split("::").pop());	//only the last element of thread id is used for filtering
			return msg;
		}
		append_to_log(node, add_attributes($("<span>").text(`${d.asctime} ${node.data.ip} [${str_pad(Array(7+1).join("\u00A0"), d.levelname)}] ${d.name} {${d.threadName}} ${d.filename}:${d.lineno}: ${d.message}`).append($("<br>"))));

		//log exception text if given
		if(d.exc_text)
		{
			//normalize line endings
			d.exc_text = (d.exc_text + "").replace(/([^>\r\n]?)(\r\n|\n\r|\r|\n)/g, "$1\n");
			//append lines to log
			$.each(d.exc_text.split("\n"), function(_, line) {
				//normalize tabs and spaces
				line = (line + "").replace(/\t/g, "\xa0\xa0\xa0\xa0");		//non breakable space
				var matches = (line + "").match(/([ ]*)([^ ].*)/);
				line = matches[1].replace(/ /g, "\xa0") + matches[2];		//non breakable space
				//append line to log
				append_to_log(node, add_attributes($("<span>").text(line).append($("<br>"))));
			});
		}
	});
	$(document).on("aps.backend.node_id", function(event, node, data) {
		node.data.uuid = data.node_id;		//update uuid value on node object
		//update uuid and ip in log and settings window
		node.data.log.find(".toolbar .node .uuid").text(data.node_id);
		node.data.log.find(".toolbar .node .host").text("("+node.data.ip+")");
		node.data.settings.find(".info .uuid .value").text(data.node_id).prop("title", data.node_id);
		node.data.settings.find(".info .host .value").text(node.data.ip);
		node.data.obj.prop("title", data.node_id);
	});
	$(document).on("aps.backend.led", function(event, node, data) {
		update_led(node, data.led_id, data.color, data.title);
	});
	$(document).on("aps.backend.data", function(event, node, data) {
		var html = $("<span>"+data.received+" <small>("+data.expected+")</small> <span>");
		if(data.received != data.expected)
			html.append("<span style=\"color: red; font-weight: bold;\">!!</span>");
		node.data.settings.find(".info .data .value").html(html)
		node.data.settings.find(".info .data").prop("title", " Received: "+data.received+" (Expected: "+data.expected+")");
	});
	$(document).on("aps.backend.state", function(event, node, data) {
		node.data.settings.find(".state .state").text(JSON.stringify(data, null, 2));
	});
	$(document).on("aps.backend.connecting aps.backend.connected aps.backend.disconnected", function(event, node, data) {
		update_connection_state(event.namespace.split(".").pop(), node.data.ip, data.addr, data.active_init, data.port);
	});
})});		//end of loaded wrapper
})();		//end of namespace wrapper