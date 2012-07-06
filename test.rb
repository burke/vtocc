$:.unshift(File.expand_path("../lib", __FILE__))
require 'vtocc'

curs = Vtocc::VtoccConnection.connect('localhost:9461', 2, dbname: 'vitess').cursor

curs.execute("SELECT * FROM trucks LIMIT 1", {})
puts "DESC{#{curs.description}}"
curs.each do |v|
  puts v.map(&:to_s).inspect
end
puts "ROWCOUNT{#{curs.rowcount}}"

# DESC{[["id", "3"], ["size", "253"]]}
# ["1", "big"]
# ROWCOUNT{1}


