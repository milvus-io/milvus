# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)

Gem::Specification.new do |s|
  s.name        = 'thrift'
  s.version     = '0.12.0'
  s.authors     = ['Thrift Developers']
  s.email       = ['dev@thrift.apache.org']
  s.homepage    = 'http://thrift.apache.org'
  s.summary     = %q{Ruby bindings for Apache Thrift}
  s.description = %q{Ruby bindings for the Apache Thrift RPC system}
  s.license     = 'Apache-2.0'
  s.extensions  = ['ext/extconf.rb']

  s.has_rdoc      = true
  s.rdoc_options  = %w[--line-numbers --inline-source --title Thrift --main README]

  s.rubyforge_project = 'thrift'

  dir = File.expand_path(File.dirname(__FILE__))

  s.files = Dir.glob("{lib,spec}/**/*")
  s.test_files = Dir.glob("{test,spec,benchmark}/**/*")
  s.executables =  Dir.glob("{bin}/**/*")

  s.extra_rdoc_files  = %w[README.md] + Dir.glob("{ext,lib}/**/*.{c,h,rb}")

  s.require_paths = %w[lib ext]

  s.add_development_dependency 'bundler',            '~> 1.11'
  s.add_development_dependency 'pry',                '~> 0.11.3'
  s.add_development_dependency 'pry-byebug',         '~> 3.6'
  s.add_development_dependency 'pry-stack_explorer', '~> 0.4.9.2'
  s.add_development_dependency 'rack',               '~> 2.0'
  s.add_development_dependency 'rack-test',          '~> 0.8.3'
  s.add_development_dependency 'rake',               '~> 12.3'
  s.add_development_dependency 'rspec',              '~> 3.7'
  s.add_development_dependency 'thin',               '~> 1.7'
end

