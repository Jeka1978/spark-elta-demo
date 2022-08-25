package com.elta

/**
 * @author Evgeny Borisov
 */
class Person(name: String, age: Int = 12) {
  override def toString: String = s"name: ${name}+age:${age}"
}
