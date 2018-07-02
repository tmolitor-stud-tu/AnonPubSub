from operator import itemgetter
import logging
logger = logging.getLogger(__name__)

# own classes
from networking import Message


class ActivePathsMixin(object):
    def __init(self, aggressive_teardown):
        self.__aggressive_teardown = aggressive_teardown
        self.__active_edges = {}
        # needed to send out error messages when needed and to know if overlay is established
        self.__reverse_edges = {}
    
    def __dump_state(self):
        return {
            "active_edges": self.__active_edges,
            "reverse_edges": self.__reverse_edges,
        }
    
    def __init_channel(self, channel):
        if channel not in self.__active_edges:
            self.__active_edges[channel] = {}
        if channel not in self.__reverse_edges:
            self.__reverse_edges[channel] = {}

    def __cleanup(self, connection):
        peer = connection.get_peer_id()
        
        # inform affected subscribers of broken path, so they can reestablish the overlay
        for channel in self.__reverse_edges:
            for subscriber in list(self.__reverse_edges[channel].keys()):
                for publisher in list(self.__reverse_edges[channel][subscriber].keys()):
                    if peer == self.__reverse_edges[channel][subscriber][publisher]["peer"]:
                        self._route_covert_data(Message("%s_error" % self.__class__.__name__, {
                            "channel": channel,
                            "subscriber": subscriber,
                            "publisher": publisher
                        }), connection)
        
        # teardown broken path to publishers along the reverse active edges if the corresponding active edge is broken
        for channel in self.__active_edges:
            for subscriber in list(self.__active_edges[channel].keys()):
                if self.__active_edges[channel][subscriber]["peer"] == peer and subscriber in self.__reverse_edges[channel]:
                    for publisher in list(self.__reverse_edges[channel][subscriber].keys()):
                        self._route_covert_data(Message("%s_teardown" % self.__class__.__name__, {
                            "channel": channel,
                            "subscriber": subscriber,
                            "publisher": publisher,
                            "version": self.__reverse_edges[channel][subscriber][publisher]["version"]
                        }))
        
        # clean up active edges to subscribers
        for channel in self.__active_edges:
            for subscriber in list(self.__active_edges[channel].keys()):
                if self.__active_edges[channel][subscriber]["peer"] == peer:
                    del self.__active_edges[channel][subscriber]
        
        # clean up reverse active edges to publishers
        for channel in self.__reverse_edges:
            for subscriber in self.__reverse_edges[channel]:
                for publisher in list(self.__reverse_edges[channel][subscriber].keys()):
                    if self.__reverse_edges[channel][subscriber][publisher]["peer"] == peer:
                        del self.__reverse_edges[channel][subscriber][publisher]
    
    def __get_next_hops(self, channel, incoming_peer=None):
        # calculate list of next nodes to route a (data) messages to according to the active edges (and don't return our incoming peer here)
        return [con for node_id, con in self.connections.items() if
                node_id in set(itemgetter("peer")(entry) for entry in self.__active_edges[channel].values()) and
                node_id != incoming_peer]
    
    def __edges_active(self, channel, subscriber):
        return (subscriber in self.__reverse_edges[channel] and
        len(set(itemgetter("peer")(entry) for entry in self.__reverse_edges[channel][subscriber].values())))
    
    def __get_known_publishers(self, channel):
        retval = set()
        for subscriber in self.__reverse_edges[channel]:
            retval.update(self.__reverse_edges[channel][subscriber].keys())
        return retval
    
    def __activate_edge(self, channel, subscriber, publisher, version, incoming_connection, outgoing_connection):
        self.__init_channel(channel)
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        outgoing_peer = outgoing_connection.get_peer_id() if outgoing_connection else None
        
        # fill in reverse edge if this activation was received from a real peer (in contrast to activations starting/originating here)
        if incoming_connection:
            if subscriber not in self.__reverse_edges[channel]:
                self.__reverse_edges[channel][subscriber] = {}
            if publisher not in self.__reverse_edges[channel][subscriber]:
                logger.info("Activation for channel '%s', version %s, edge pointing from publisher '%s' (connection: %s) to subscriber '%s' (connection:  %s)..." % (
                    str(channel),
                    str(version),
                    str(publisher),
                    str(incoming_connection),
                    str(subscriber),
                    str(outgoing_connection)
                ))
            # send out a teardown message if a new path was detected
            elif (incoming_peer != self.__reverse_edges[channel][subscriber][publisher]["peer"] and
            version > self.__reverse_edges[channel][subscriber][publisher]["version"]):
                logger.warning("New path activation for channel '%s' version %s, edge pointing from publisher '%s' (connection: %s) to subscriber '%s' (connection:  %s)..." % (
                    str(channel),
                    str(version),
                    str(publisher),
                    str(incoming_connection),
                    str(subscriber),
                    str(outgoing_connection)
                ))
                if self.__aggressive_teardown:
                    self._route_covert_data(Message("%s_teardown" % self.__class__.__name__, {
                        "channel": channel,
                        "subscriber": subscriber,
                        "publisher": publisher,
                        # this has to be the version of the old edge, not the one of our new edge!
                        "version": self.reverse_edges[channel][subscriber][publisher]["version"]
                    }))
            # always increment edge versions, never decrement
            if (publisher not in self.__reverse_edges[channel][subscriber] or
            version >= self.__reverse_edges[channel][subscriber][publisher]["version"]):
                logger.info("Edge version is now %d (from %s to %s)" % (version, str(incoming_connection), str(outgoing_connection)))
                self.__reverse_edges[channel][subscriber][publisher] = {
                    "version": version,
                    "peer": incoming_peer
                }
        
        if outgoing_peer:
            # only activate (new) edge if we don't have an edge to this subscriber yet, or if the new edge has a greater version
            # than the edge we have already
            if (subscriber not in self.__active_edges[channel] or
            self.__active_edges[channel][subscriber]["version"] < version):
                #NOTE: use this for fewer logging output: if outgoing_peer not in set(itemgetter("peer")(entry) for entry in self.__active_edges[channel].values()):
                logger.info("Activating edge from %s to %s for channel '%s' (new edge version: %d, old edge version: %d)..." % (
                    str(incoming_connection),
                    str(outgoing_connection),
                    str(channel),
                    version,
                    self.__active_edges[channel][subscriber]["version"] if subscriber in self.__active_edges[channel] else 0
                ))
                self.__active_edges[channel][subscriber] = {
                    "version": version,
                    "peer": outgoing_peer
                }
            else:
                logger.info("Ignoring new edge version %d, old edge has version %d" % (version, self.__active_edges[channel][subscriber]["version"]))
    
    def __route_teardown(self, teardown, incoming_connection):
        logger.info("Routing teardown: %s coming from %s..." % (str(teardown), str(incoming_connection)))
        self.__init_channel(teardown["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        
        # get peer id of reverse active edge to this publisher for this subscriber and delete edge afterwards
        # but only if the edge version is lower or equal to the teardown version
        node_id = None
        if teardown["subscriber"] in self.__reverse_edges[teardown["channel"]]:
            if teardown["publisher"] in self.__reverse_edges[teardown["channel"]][teardown["subscriber"]]:
                node_id = self.__reverse_edges[teardown["channel"]][teardown["subscriber"]][teardown["publisher"]]["peer"]
                edge_version = self.__reverse_edges[teardown["channel"]][teardown["subscriber"]][teardown["publisher"]]["version"]
                logger.info("Teardown versus edge: edge: %d, teardown: %d" % (edge_version, teardown["version"]))
                if edge_version > teardown["version"]:
                    logger.warning("Teardown version (%d) lower than edge version (%d), not processing this outdated teardown!" % (teardown["version"], edge_version))
                    return      # ignore outdated teardown messages not originating from here
                logger.info("Removing reverse active edge '%s' due to teardown..." % str(self.__reverse_edges[teardown["channel"]][teardown["subscriber"]][teardown["publisher"]]))
                del self.__reverse_edges[teardown["channel"]][teardown["subscriber"]][teardown["publisher"]]
                if not len(self.__reverse_edges[teardown["channel"]][teardown["subscriber"]]):
                    del self.__reverse_edges[teardown["channel"]][teardown["subscriber"]]
        
        # only handle teardown if we have a matching active edge
        # otherwise don't touch our active edge and don't route the teardown further
        # (but only if the teardown does not originate here, because we definitely need to route it further if it originates here)
        if incoming_connection and (teardown["subscriber"] not in self.__active_edges[teardown["channel"]] or
        self.__active_edges[teardown["channel"]][teardown["subscriber"]]["peer"] != incoming_peer or
        self.__active_edges[teardown["channel"]][teardown["subscriber"]]["version"] > teardown["version"]):
            logger.info("Not routing teardown further because we have no matching active edge (or its version is higher than the teardown version)...")
            return
        
        # delete active edge to this subscriber if the teardown didn't originate here AND the reverse active edge
        # for this subscriber is the only one (or if there is no reverse active edge at all because the reverse path ends here).
        # if it is NOT the only one, we are a forwarder for another publisher and the active edge is still needed
        if incoming_connection and ((teardown["subscriber"] in self.__reverse_edges[teardown["channel"]] and 
        len(self.__reverse_edges[teardown["channel"]][teardown["subscriber"]]) <= 1) or
        teardown["subscriber"] not in self.__reverse_edges[teardown["channel"]]):
            logger.info("Removing active edge via %s due to teardown..." % (
                str(self.connections[self.__active_edges[teardown["channel"]][teardown["subscriber"]]["peer"]]) if
                self.__active_edges[teardown["channel"]][teardown["subscriber"]]["peer"] in self.connections else
                str(self.__active_edges[teardown["channel"]][teardown["subscriber"]]["peer"])
            ))
            del self.__active_edges[teardown["channel"]][teardown["subscriber"]]
        
        # route teardown message further along the reverse active edge to this publisher for this subscriber
        if node_id in self.connections:
            logger.info("Routing teardown to %s..." % str(self.connections[node_id]))
            self._send_covert_msg(teardown, self.connections[node_id])
    
    def __route_error(self, error, incoming_connection, callback=None):
        logger.info("Routing error: %s coming from %s..." % (str(error), str(incoming_connection)))
        self.__init_channel(error["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        
        # recreate broken overlay if needed (we subscribed the channel and incoming_peer is a reverse active edge)
        # edge versions are not relevant here
        if error["channel"] in self.subscriptions and error["subscriber"] == self.subscriber_ids[error["channel"]]:
            if incoming_peer in set(itemgetter("peer")(entry) for entry in
            self.__reverse_edges[error["channel"]][error["subscriber"]].values()):
                logger.warning("Received error, tearing down broken path to publisher %s on channel '%s'..." % (str(error["publisher"]), str(error["channel"])))
                self._route_covert_data(Message("%s_teardown" % self.__class__.__name__, {
                    "channel": error["channel"],
                    "subscriber": error["subscriber"],
                    "publisher": error["publisher"],
                    "version": self.__reverse_edges[error["channel"]][error["subscriber"]][error["publisher"]]["version"]
                }))
                # handle error further if a callback was given
                if callback:
                    callback(error)
        
        # route error message further along the active edge for this subscriber
        if error["subscriber"] in self.__active_edges[error["channel"]]:
            node_id = self.__active_edges[error["channel"]][error["subscriber"]]["peer"]
            if node_id in self.connections:
                logger.info("Routing error to %s..." % str(self.connections[node_id]))
                self._send_covert_msg(error, self.connections[node_id])
    
    def __route_unsubscribe(self, unsubscribe, incoming_connection):
        logger.info("Routing unsubscribe: %s coming from %s..." % (str(unsubscribe), str(incoming_connection)))
        self.__init_channel(unsubscribe["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        
        # route unsubscribe message along all reverse active edges to all publishers
        # edge versions not elevant here
        for node_id in set(itemgetter("peer")(entry) for entry in
        self.__reverse_edges[unsubscribe["channel"]][unsubscribe["subscriber"]].values()):
            if node_id in self.connections:
                logger.info("Routing unsubscribe to %s..." % str(self.connections[node_id]))
                self._send_covert_msg(error, self.connections[node_id])
        
        # delete active edge to this subscriber
        if unsubscribe["subscriber"] in self.__active_edges[unsubscribe["channel"]]:
            del self.__active_edges[unsubscribe["channel"]][unsubscribe["subscriber"]]
        
        # delete all reverse active edges of this subscriber (it unsubscribed cleanly)
        # edge versions not relevant here
        if unsubscribe["subscriber"] in self.__reverse_edges[unsubscribe["channel"]]:
            del self.__reverse_edges[unsubscribe["channel"]][unsubscribe["subscriber"]]
    
