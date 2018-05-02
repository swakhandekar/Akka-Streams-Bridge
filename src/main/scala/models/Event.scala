package models

case class Event(val list: List[DBChange] = List()) {
  def addToList(element: DBChange): Event = {
    copy(element :: list)
  }

}
