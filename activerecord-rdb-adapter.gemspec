Gem::Specification.new do |s|
  s.author = 'Andrey Lobanov (RedSoft)'
  s.name = 'activerecord-rdb-adapter'
  s.version = '0.9.1'
  s.date = '2019-09-12'
  s.summary = 'ActiveRecord RedDatabase 3+ and Firebird 3+ Adapter'
  s.description = 'ActiveRecord RedDatabase 3+ and Firebird 3+ Adapter for Rails 5+'
  s.licenses = ['MIT']
  s.requirements = 'Firebird library fb'
  s.require_paths = ['.', 'lib']
  s.email = 'andrey.lobanov@red-soft.ru'
  s.homepage = 'https://github.com/woblavobla/activerecord_rdb_adapter'
  s.files = Dir['README.md', 'lib/**/*', 'extconf.rb', 'fb.c', 'fb_extensions.rb']
  s.extensions = ['extconf.rb'] if s.platform == Gem::Platform::RUBY

  s.add_dependency 'rails', '~> 5.2'
end
