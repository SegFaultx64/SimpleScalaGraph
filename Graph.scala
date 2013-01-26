
package org.segfaultx.graph

class Graph(val links: List[(Int, Int)], val metadata: Map[Int, Map[String, String]], val indices: List[Int]) {

  def this() = this(List empty, Map empty, List empty)

  /** Gets the next availible index and returns it as a string
  */
  def nextIndex: Int = {
    var lastVal = -1
    val result = indices.toStream.takeWhile { a =>
      val continue = (a - lastVal == 1)
      lastVal = a
      continue
    }
    lastVal + 1
  }

  /** Inserts a new blank node
  *
  * Creates a new map with only the element _ID -> the ID of the new node and returns the id and the new graph as a tuple
  */
  private def ++(): (Int, Graph) = {
    val index = nextIndex
    (index, new Graph(links, metadata.updated(index, Map("_ID" -> index.toString)), index :: indices))
  }

  /** Inserts a non-blank new node and returns the ID
  *
  * Iterates through a map of Strings onto JsValues and for each one it converts them into the proper format
  * and stores them in the newly created node.
  */
  def +(data: Map[String, String]): Graph = {
    val g = (this++)
    g._2.updateRecord(g._1, data)
  }

  /** Updates an existing record returns the ID of the updated record
  *
  * Iterates through a map of Strings onto JsValues and for each one it converts them into the proper format
  * and stores them in the node with the passed in ID
  */
  def updateRecord(id: Int, data: Map[String, String]): Graph = {
    data.keySet.foldLeft(this) ((a,b) => a.setMetadata(id, b, data(b)))
  }

  /** Creates a link passed on the parameters
  *
  * Returns Unit
  */
  def createLink(from: Int, to: Int): Graph = {
    new Graph((from, to) :: links, metadata, indices)
  }

  /** Sets the metadata stored at a key on a specific node
  *
  * Returns Unit
  */
  def setMetadata(id: Int, key: String, value: String): Graph = {
    val toInsert: Map[String, String] = metadata(id).updated(key, value)
    new Graph(links, metadata.updated(id, toInsert), indices)
  }

  /** Extracts a single node representing the given id
  *
  * Returns a map representation of the node
  */
  def \(id: Int) = {
   if (!(metadata.contains(id))) {
      None
    } else {
      Some(metadata(id))
    }
  }

  /** Extracts a sequence of nodes given a list of IDs
  *
  * Returns a list of the nodes
  */
  def \(id: List[Int]) = {
    var toReturn: List[Map[String, String]] = List empty;
    for (curId <- id) {
      val metadataToInsert = metadata(curId)
      toReturn ::=  metadataToInsert
    }
    Some(toReturn)
  }

  /** Extracts a sequence of nodes given a list of IDs
  *
  * Returns a list of the nodes, uses a custom ordering
  */
  def \(id: List[Int], sortBy: Ordering[Map[String, String]]) = {
    var toReturn: List[Map[String, String]] = List empty;
    for (curId <- id) {
      val metadataToInsert = metadata(curId)
      toReturn ::=  metadataToInsert
    }
    toReturn = toReturn.sorted(sortBy)
    val jsonObject = ""
    Some(jsonObject)
  }

  /** Deletes a link and returns the new Graph */
  def deleteLink(from: Int, to: Int) = {
    new Graph(links diff List(from, to), metadata, indices)
  }

  /** Deletes a whole node and returns a new Graph */
  def -(id: Int) = {
    new Graph(links.filter(a => (a._1 != id && a._2 != id)), metadata.filter(a => (a._2("_ID") != id)), indices diff List(id))
  }

  /** Deletes a single member of a node and returns a new Graph */
  def --(id: Int, field: String) = {
    val member = field toLowerCase;
    new Graph(links, metadata.updated(id, (metadata(id) - member)), indices)
  }

  /** Returns a list of tuples representing the incoming links of a node */
  def getLinksTo(to: Int): List[(Int, Int)] = {
    links.filter(a => a._2 == to)
  }

  /** Returns a list of tuples representing the outgoing links of a node */
  def getLinksFrom(from: Int): List[(Int, Int)] = {
    links.filter(a => a._1 == from)
  }

  /** Returns a boolean determining if a link exists */
  def linkExists(from: Int, to: Int): Boolean = {
    links.exists(a => a == (from, to))
  }

  /** Takes a start node and a depth extracts a subgraph obeying directed edges flow
  *
  * Returns a list of the IDs of the nodes that are included in the subgraph as a list
  * of Ints.
  */
  def extractSubDiGraph(startNode: Int, depth: Int): List[Int] = {
    if (depth <= 0) { return List empty }
    var toReturn: List[Int] = List(startNode)
    var visited: List[Int] = List empty
    var curNode: Int = startNode
    var toVisit: List[Int] = links.filter(a => a._1 == startNode).map(a => a._2)
    for (x <- 1 to depth) {
      var visitNext: List[Int] = List empty;
      for (a <- toVisit) {
        curNode = a
        visited ::= curNode
        toReturn ::= curNode
        visitNext = visitNext ++ links.filter(a => { a._1 == curNode && !(visited.contains(a._2)) }).map(a => a._2)
      }
      toVisit = visitNext
    }
    toReturn
  }

  /** Takes a start node and a depth extracts a subgraph without regard for edge direction
  *
  * Returns a list of the IDs of the nodes that are included in the subgraph as a list
  * of Ints.
  */
  def extractSubGraph(startNode: Int, depth: Int): List[Int] = {
    if (depth <= 0) { return List empty }
    var toReturn: List[Int] = List(startNode)
    var visited: List[Int] = List(startNode)
    var curNode: Int = startNode
    var toVisit: List[Int] = links.filter(a => a._1 == startNode).map(a => a._2)
    toVisit = links.filter(a => { a._2 == startNode || a._1 == startNode}).map(a => {
      if (a._1 == startNode) {a._2} else if (a._2 == startNode) {a._1} else {-1}
    })
    for (x <- 1 to depth) {
      var visitNext: List[Int] = List empty;
      for (a <- toVisit) {
        curNode = a
        visited ::= curNode
        toReturn ::= curNode
        visitNext = visitNext ++ links.filter(a => { (a._2 == curNode && !(visited.contains(a._1))) ||
          (a._1 == curNode && !(visited.contains(a._2)))}).map(a => {
            if (a._1 == curNode) {a._2} else if (a._2 == curNode) {a._1} else {-1}
        })
      }
      toVisit = visitNext
    }
    toReturn
  }

  /** Takes and field -> query string and a comparison algorithim, returns a list of IDs that match */
  def search(field: String, query: String, algorithim: (String, String) => Option[Boolean]): List[Int] = {
    if (field == "" && query == "") {
      metadata.map(a => a._1).toList
    } else {
      metadata.filter(a => {
        a._2.filter(b => {
          val comp: (String, String) => Option[Boolean] = this.regexMatch;
          comp(b._1, field) match {
            case Some(true) => {
              algorithim(b._2, query) match {
                case Some(c) => c
                case None => false
              }
            }
            case _ => false
          }
        }) != Map.empty
      })
      .map(a => a._1).toList
    }
  }

  /** Takes and field -> query string, a comparison algorithim and, a list of filter functions that map Ints onto booleans
  *
  * Returns a list of IDs that match as a list of Ints
  */
  def search(field: String, query: String, algorithim: (String, String) => Option[Boolean], filters: List[(Int) => Boolean]): List[Int] = {
    if (field == "" && query == "") {
      metadata.par.map(a => a._1).toList
    } else {
      val start = System.currentTimeMillis
      val searchResults = metadata.par.filter(a => {
	       a._2.par.filter(b => {
	         val comp: (String, String) => Option[Boolean] = this.regexMatch;
	         comp(b._1, field) match {
	           case Some(true) => {
	             algorithim(b._2, query) match {
		              case Some(c) => c
		              case None => false
                }
              }
            case _ => false
	         }
	       }) != Map.empty
      }).par.map(a => a._1)
      val end = System.currentTimeMillis
      searchResults.par.filter(a => {
       filters.par.map { _(a) }.par.reduceLeft { _ && _ }
      }).toList
    }
  }

  /** A simple regex match algorithim for use with searchs */
  def regexMatch(data: String, regex: String): Option[Boolean] = {
    regex.r findFirstMatchIn data match {
      case Some(_) => Some(true)
      case None => None
    }
  }

  /** Generates a map where the keys a strings containing keys from the graph and the values being the number of occurences of that key */
  def getCommonFields(): Map[String, Int] = {
    val fields = metadata.map(a => a._2.keySet).toList.flatten
    fields.foldLeft(Map[String, Int]() withDefaultValue 0) {
      (m, x) => m + (x -> (1 + m(x)))
    }
  }

}
