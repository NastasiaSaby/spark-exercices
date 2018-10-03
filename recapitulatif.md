# Récapitulatif
## Scala variable

```scala
val age = 12
println(age) 
```
12

## Scala fonction

```scala
def speak() = {
  println("salut")
}

speak()
```
salut

```scala
def speak(word: String) = {
  println(word)
}

speak("hello ")
```
hello

```scala
def sendWord(word: String) = {
  "hello " + word
}

println(sendWord(" world"))
```
hello world

## Case class

```scala
case class Personne(age: Int, name: String)
val personne1 = Personne(12, "Lucien")

println(personne1.age)
```
12
