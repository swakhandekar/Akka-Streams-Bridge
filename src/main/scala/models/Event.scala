package models

case class Event(list: List[DBChange] = List()) {
  def addToList(element: DBChange): Event = {
    copy(element :: list)
  }

}
