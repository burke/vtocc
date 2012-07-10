$:.unshift(File.expand_path("../lib", __FILE__))
require 'vtocc'

conn = Vtocc::VtoccConnection.connect('localhost:9461', 2, dbname: 'vitess')
curs = conn.cursor
require'pry';binding.pry

2500.times {
  curs.execute("SELECT * from vtocc_a")
  curs.execute("SELECT * from vtocc_a WHERE id = ?", [1])
  curs.execute("SELECT * from vtocc_a WHERE id = :some_id", {some_id: 1})
  curs.execute("SELECT * from vtocc_a WHERE id = %s", [1])
}



puts "DESC{#{curs.description}}"
puts "ROWCOUNT{#{curs.rowcount}}"


