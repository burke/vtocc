# this code is 90% borrowed from activerecord 3.2.
# It takes a sql query in any of the forms listed below and 
# returns a named substition query suitable for consumption by vtocc.
#
# Supported input formats:
#   ["select * from trucks where size = ?", ['monster']]
#   ["select * from trucks where size = 'monster'"]
#   ["select * from trucks where size = :size], {size: 'monster'}]
#   ["select * from trucks where size = %s", ['monster']]

module Vtocc
  module QueryReshaper

    def self.call(ary)
      statement, *values = ary

      if values.first.is_a?(Hash) && statement =~ /:\w+/
        # Happy day, there's nothing to do -- this is the format vtocc wants already!
        sql = statement
        bind_variables = values.first
      elsif statement.include?('?')
        sql, bind_variables = replace_bind_variables(statement, values)
      elsif statement.nil? || statement == ''
        sql = statement
        bind_variables = {}
      else
        # convert from printf form to ? form
        statement = statement % (['?'] * statement.count('%'))
        if values.size == 1 && values[0].empty?
          values = []
        end
        sql, bind_variables = replace_bind_variables(statement, values)
      end

      [sql, bind_variables]
    end

    private

    def self.replace_bind_variables(statement, values)
      raise_if_bind_arity_mismatch(statement, statement.count('?'), values.size)

      bind_index = 0
      sql = statement.gsub('?') { ":bind_#{bind_index += 1}" }
      bind_variables = Hash[
        values.each.with_index.map { |x, i|
          ["bind_#{i + 1}", x]
        }
      ]

      [sql, bind_variables]
    end

    def self.raise_if_bind_arity_mismatch(statement, expected, provided)
      unless expected == provided
        raise OperationalError, "wrong number of bind variables (#{provided} for #{expected}) in: #{statement}"
      end
    end

  end

end
