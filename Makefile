TESTS = test/*.js
test:
	mocha --timeout 5000 --check-leaks --reporter nyan $(TESTS)
 
.PHONY: test