4.times {

  fork {
    `ruby ./test.rb`
  }
}
Process.waitall
