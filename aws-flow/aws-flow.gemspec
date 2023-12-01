require './lib/aws/decider/version'

Gem::Specification.new do |s|
  s.name        = 'aws-flow'
  s.version     = AWS::Flow::version
  s.date        = Time.now
  s.license     = "Apache-2.0"
  s.summary     = "AWS Flow Framework for Ruby"
  s.description = "Library to provide the AWS Flow Framework for Ruby"
  s.authors     = "Michael Steger, Paritosh Mohan, Jacques Thomas"
  s.executables = ["aws-flow-ruby", "aws-flow-utils"]
  s.homepage 	= "https://aws.amazon.com/swf/details/flow/"
  s.email       = ''
  s.files       = `git ls-files`.split("\n").reject {|file| file =~ /aws-flow-core/}
  s.require_paths << "lib/aws/"
  s.required_ruby_version = ">= 1.9.1"
  s.add_dependency "aws-sdk-v1"
  s.add_dependency "lru_redux", "~> 0", ">= 0.8.1"
  s.add_dependency "json", "~> 2.6.3"
  s.add_dependency "nokogiri"
  s.add_dependency "bugsnag"
  s.add_development_dependency('pry')
end
