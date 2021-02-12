//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_function_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_compiler.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ScalarFunctionCatalogEntry;

//! Represents a function call that has been bound to a base function
class BoundFunctionExpression : public Expression {
public:
	BoundFunctionExpression(LogicalType return_type, ScalarFunction bound_function,
	                        vector<unique_ptr<Expression>> arguments, unique_ptr<FunctionData> bind_info,
	                        bool is_operator = false);

	// The bound function expression
	ScalarFunction function;
	//! The compiled expression (if any) that replaces the bound function expression during execution.
	//! Must be a shared_ptr since CompiledExpression is a move-only type
	shared_ptr<CompiledExpression> compiled_expression;
	//! List of child-expressions of the function
	vector<unique_ptr<Expression>> children;
	//! The bound function data (if any)
	unique_ptr<FunctionData> bind_info;
	//! Whether or not the function is an operator, only used for rendering
	bool is_operator;

public:
	bool HasSideEffects() const override;
	bool IsFoldable() const override;
	string ToString() const override;

	hash_t Hash() const override;
	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
