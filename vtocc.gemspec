# -*- encoding: utf-8 -*-
require File.expand_path('../lib/vtocc/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Burke Libbey"]
  gem.email         = ["burke@libbey.me"]
  gem.description   = %q{Vitess client for ruby}
  gem.summary       = %q{Vitess client for ruby}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "vtocc"
  gem.require_paths = ["lib"]
  gem.version       = Vtocc::VERSION

  gem.add_dependency 'bson'
  gem.add_dependency 'bson_ext'
end
