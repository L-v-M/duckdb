#include "duckdb/execution/expression_compiler.hpp"

#if defined(__linux__)

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/fstream.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/execution/operator/helper/physical_prepare.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/bound_tokens.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

#include <dlfcn.h>
#include <iostream>
#include <sstream>
#include <sys/types.h>
#include <sys/wait.h>

namespace duckdb {

void ExpressionCompiler::CompileExpressions(PhysicalOperator &op) {
	CompileOperatorChildrenExpressions(op);
	CompileOperatorExpressions(op);
}

static uint64_t GetCompilationTimeRec(Expression &expression) {
	uint64_t result = 0;
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &bfe = static_cast<BoundFunctionExpression &>(expression);
		if (bfe.compiled_expression && bfe.compiled_expression->IsReady()) {
			result += bfe.compiled_expression->GetCompilationTime();
		}
	}

	ExpressionIterator::EnumerateChildren(
	    expression, [&result](Expression &child_expression) { result += GetCompilationTimeRec(child_expression); });
	return result;
}

uint64_t ExpressionCompiler::GetCompilationTime(PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::PROJECTION) {
		uint64_t result = 0;
		for (auto &expression : static_cast<PhysicalProjection &>(op).select_list) {
			result += GetCompilationTimeRec(*expression);
		}
		return result;
	}
	return 0;
}

void ExpressionCompiler::CompileOperatorChildrenExpressions(PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::PREPARE) {
		CompileExpressions(*static_cast<PhysicalPrepare &>(op).prepared->plan);
	}

	for (auto &child : op.children) {
		CompileExpressions(*child);
	}
}

void ExpressionCompiler::CompileOperatorExpressions(PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::PROJECTION) {
		for (auto &expression : static_cast<PhysicalProjection &>(op).select_list) {
			CompileExpression(*expression);
		}
	}
}

void ExpressionCompiler::CompileExpression(Expression &expression) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &unfused_children_expressions = CompileExpression(static_cast<BoundFunctionExpression &>(expression));
		for (auto &expr : unfused_children_expressions) {
			CompileExpression(*expr);
		}
	} else {
		ExpressionIterator::EnumerateChildren(
		    expression, [this](Expression &child_expression) { CompileExpression(child_expression); });
	}
}

class UnsupportedTypeException : public Exception {
public:
	UnsupportedTypeException() : Exception("Physical type is not supported") {
	}
};

//! Convert a PhysicalType to a type name that can be used in a C++ program
static string TypeToString(PhysicalType type) {
	switch (type) {
	case PhysicalType::UINT8:
		return "uint8_t";
	case PhysicalType::INT8:
		return "int8_t";
	case PhysicalType::UINT16:
		return "uint16_t";
	case PhysicalType::INT16:
		return "int16_t";
	case PhysicalType::UINT32:
		return "uint32_t";
	case PhysicalType::INT32:
		return "int32_t";
	case PhysicalType::UINT64:
		return "uint64_t";
	case PhysicalType::INT64:
		return "int64_t";
	case PhysicalType::FLOAT:
		return "float";
	case PhysicalType::DOUBLE:
		return "double";
	case PhysicalType::INT128:
		return "hugeint_t";
	default:
		throw UnsupportedTypeException {};
	}
}

//! Check if compilation of the BoundFunctionExpression is supported
static bool IsExpressionCompilable(BoundFunctionExpression &expr) {
	auto supported_operators = {"+", "-", "*", "/"};
	return !expr.IsFoldable() && !expr.HasSideEffects() && expr.is_operator && !expr.IsAggregate() &&
	       !expr.IsWindow() && !expr.HasSubquery() && !expr.IsScalar() && !expr.HasParameter() &&
	       find(supported_operators.begin(), supported_operators.end(), expr.function.name) !=
	           supported_operators.end();
}

struct ExpressionInfo {
	explicit ExpressionInfo(BoundFunctionExpression &func_expr) {
		std::ostringstream expression_code;
		ConstructExpressionInfoImpl(func_expr, expression_code);
		expression = move(expression_code).str();
	}
	vector<unique_ptr<Expression>> input_expressions;
	string expression;

private:
	void ConstructExpressionInfoImpl(BoundFunctionExpression &func_expr, std::ostringstream &expression_code) {
		expression_code << "static_cast<" << TypeToString(func_expr.return_type.InternalType()) << ">(";

		// Handle unary functions
		if (func_expr.children.size() == 1) {
			expression_code << func_expr.function.name;
		}

		for (idx_t i = 0; i != func_expr.children.size(); ++i) {
			// Handle n-ary functions
			if (i > 0) {
				expression_code << " " << func_expr.function.name << " ";
			}

			if (func_expr.children[i]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
				auto &child_expression = *static_cast<BoundFunctionExpression *>(func_expr.children[i].get());
				if (IsExpressionCompilable(child_expression)) {
					ConstructExpressionInfoImpl(child_expression, expression_code);
					continue;
				}
			}

			expression_code << "entry" << input_expressions.size();
			input_expressions.emplace_back(func_expr.children[i]->Copy());
		}

		expression_code << ")";
	}
};

vector<unique_ptr<Expression>> &ExpressionCompiler::CompileExpression(BoundFunctionExpression &expr) {
	// Check if the expression can be compiled
	if (!IsExpressionCompilable(expr)) {
		return expr.children;
	}

	try {
		ExpressionInfo expression_info {expr};
		if (expression_info.input_expressions.size() > 2) {
			std::ostringstream code;

			// Include directives
			code << "#include \"duckdb/execution/expression_executor_state.hpp\"\n"
			     << "#include \"duckdb/common/algorithm.hpp\"\n"
			     << "using namespace duckdb;\n";

			{ // ExecuteConstant
				code << "static void ExecuteConstant(DataChunk &input, Vector &result) {\n"
				     << "result.vector_type = VectorType::CONSTANT_VECTOR;\n";

				for (uint64_t i = 0; i != expression_info.input_expressions.size(); ++i) {
					auto type_name = TypeToString(expression_info.input_expressions[i]->return_type.InternalType());
					code << "auto input_data" << i << " = ConstantVector::GetData<" << type_name << ">(input.data[" << i
					     << "]);\n";
				}

				auto return_type_name = TypeToString(expr.return_type.InternalType());
				code << "auto result_data = ConstantVector::GetData<" << return_type_name << ">(result);\n";

				code << "if (any_of(input.data.begin(), input.data.end(), "
				     << "[](const Vector &vec) { return ConstantVector::IsNull(vec); })) {\n"
				     << "ConstantVector::SetNull(result, true);\n"
				     << "return;\n"
				     << "}\n";

				for (uint64_t i = 0; i != expression_info.input_expressions.size(); ++i) {
					code << "auto entry" << i << " = *input_data" << i << ";\n";
				}

				code << "*result_data = " << expression_info.expression << ";\n}\n";
			}

			{ // ExecuteFlatLoop
				code << "static void ExecuteFlatLoop(";
				for (uint64_t i = 0; i != expression_info.input_expressions.size(); ++i) {
					auto type_name = TypeToString(expression_info.input_expressions[i]->return_type.InternalType());
					code << type_name << "*__restrict input_data" << i << ", "
					     << "bool is_input_data" << i << "_constant, ";
				}
				auto return_type_name = TypeToString(expr.return_type.InternalType());
				code << return_type_name << "*__restrict result_data"
				     << ", idx_t count, nullmask_t &nullmask) {\n"
				     << "if (nullmask.any()) {\n"
				     << "for (idx_t i = 0; i < count; i++) {\n"
				     << "if (!nullmask[i]) {\n";
				for (uint64_t i = 0; i != expression_info.input_expressions.size(); ++i) {
					code << "auto entry" << i << " = input_data" << i << "[";
					if (expression_info.input_expressions[i]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
						code << "0";
					} else if (expression_info.input_expressions[i]->GetExpressionClass() ==
					           ExpressionClass::BOUND_REF) {
						code << "i";
					} else {
						code << "is_input_data" << i << "_constant ? 0 : i";
					}
					code << "];\n";
				}
				code << "result_data[i] = " << expression_info.expression << ";\n}\n}\n";
				code << "} else {\n"
				     << "for (idx_t i = 0; i < count; i++) {\n";
				for (uint64_t i = 0; i != expression_info.input_expressions.size(); ++i) {
					code << "auto entry" << i << " = input_data" << i << "[";
					if (expression_info.input_expressions[i]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
						code << "0";
					} else if (expression_info.input_expressions[i]->GetExpressionClass() ==
					           ExpressionClass::BOUND_REF) {
						code << "i";
					} else {
						code << "is_input_data" << i << "_constant ? 0 : i";
					}
					code << "];\n";
				}
				code << "result_data[i] = " << expression_info.expression << ";\n}\n}\n}\n";
			}

			{ // ExecuteFlat
				code << "static void ExecuteFlat(DataChunk &input, Vector &result) {\n";
				for (uint64_t i = 0; i != expression_info.input_expressions.size(); ++i) {
					auto type_name = TypeToString(expression_info.input_expressions[i]->return_type.InternalType());
					code << "auto input_data" << i << " = FlatVector::GetData<" << type_name << ">(input.data[" << i
					     << "]);\n";
				}

				code << "if (any_of(input.data.begin(), input.data.end(), "
				     << "[](const Vector &vec) { return vec.vector_type == VectorType::CONSTANT_VECTOR && "
				     << "ConstantVector::IsNull(vec); })) {\n"
				     << "result.vector_type = VectorType::CONSTANT_VECTOR;\n"
				     << "ConstantVector::SetNull(result, true);\n"
				     << "return;\n}\n";

				auto return_type_name = TypeToString(expr.return_type.InternalType());
				code << "result.vector_type = VectorType::FLAT_VECTOR;\n"
				     << "auto result_data = FlatVector::GetData<" << return_type_name << ">(result);\n";

				code << "nullmask_t mask;\n"
				     << "for (auto &vec : input.data) {\n"
				     << "if (vec.vector_type == VectorType::FLAT_VECTOR) {\n"
				     << "mask |= FlatVector::Nullmask(vec);\n}\n}\n"
				     << "FlatVector::SetNullmask(result, move(mask));\n";
				code << "ExecuteFlatLoop(";
				for (idx_t i = 0; i != expression_info.input_expressions.size(); ++i) {
					code << "input_data" << i << ", "
					     << "input.data[" << i << "].vector_type == VectorType::CONSTANT_VECTOR, ";
				}
				code << "result_data, input.size(), FlatVector::Nullmask(result));\n}\n";
			}

			{ // ExecuteGenericLoop
				code << "static void ExecuteGenericLoop(";
				for (idx_t i = 0; i != expression_info.input_expressions.size(); ++i) {
					auto type_name = TypeToString(expression_info.input_expressions[i]->return_type.InternalType());
					code << type_name << "*__restrict input_data" << i << ", "
					     << "const SelectionVector *__restrict input_data" << i << "_sel, "
					     << "nullmask_t &input_data" << i << "_nullmask, ";
				}
				auto return_type_name = TypeToString(expr.return_type.InternalType());
				code << return_type_name << " *__restrict result_data"
				     << ", idx_t count, nullmask_t &result_nullmask) {\n";

				code << "if (input_data0_nullmask.any()";
				for (idx_t i = 1; i != expression_info.input_expressions.size(); ++i) {
					code << " || input_data" << i << "_nullmask.any()";
				}
				code << ") {\n"
				     << "for (idx_t i = 0; i < count; i++) {\n";
				for (idx_t i = 0; i != expression_info.input_expressions.size(); ++i) {
					code << "auto index" << i << " = input_data" << i << "_sel->get_index(i);\n";
				}
				code << "if (!input_data0_nullmask[index0]";
				for (idx_t i = 1; i != expression_info.input_expressions.size(); ++i) {
					code << " && !input_data" << i << "_nullmask[index" << i << "]";
				}
				code << ") {\n";
				for (idx_t i = 0; i != expression_info.input_expressions.size(); ++i) {
					code << "auto entry" << i << " = input_data" << i << "[index" << i << "];\n";
				}
				code << "result_data[i] = " << expression_info.expression << ";"
				     << "} else {\n"
				     << "result_nullmask[i] = true;\n}\n}\n"
				     << "} else {\n"
				     << "for (idx_t i = 0; i < count; i++) {\n";
				for (idx_t i = 0; i != expression_info.input_expressions.size(); ++i) {
					code << "auto entry" << i << " = input_data" << i << "[input_data" << i << "_sel->get_index(i)];\n";
				}
				code << "result_data[i] = " << expression_info.expression << ";\n}\n}\n}\n";
			}

			{ // ExecuteGeneric
				code << "static void ExecuteGeneric(DataChunk &input, Vector &result) {\n"
				     << "auto count = input.size();\n";

				for (idx_t i = 0; i != expression_info.input_expressions.size(); ++i) {
					code << "VectorData input_data" << i << ";\n"
					     << "input.data[" << i << "].Orrify(count, input_data" << i << ");\n";
				}

				auto return_type_name = TypeToString(expr.return_type.InternalType());
				code << "result.vector_type = VectorType::FLAT_VECTOR;\n"
				     << "auto result_data = FlatVector::GetData<" << return_type_name << ">(result);\n";

				code << "ExecuteGenericLoop(";
				for (idx_t i = 0; i != expression_info.input_expressions.size(); ++i) {
					auto type_name = TypeToString(expression_info.input_expressions[i]->return_type.InternalType());
					code << "reinterpret_cast<" << type_name << "*>(input_data" << i << ".data), "
					     << "input_data" << i << ".sel, *input_data" << i << ".nullmask, ";
				}
				code << "result_data, count, FlatVector::Nullmask(result));\n}\n";
			}

			{ // main function
				code << "extern \"C\" void Execute(DataChunk &input, ExpressionState &state, Vector &result) {\n"
				     << "if (all_of(input.data.begin(), input.data.end(), "
				     << "[](const Vector &vec) { return vec.vector_type == VectorType::CONSTANT_VECTOR; })) {\n"
				     << "ExecuteConstant(input, result);\n"
				     << "} else if (all_of(input.data.begin(), input.data.end(), "
				     << "[](const Vector &vec) { return vec.vector_type == VectorType::FLAT_VECTOR || "
				        "vec.vector_type "
				        "== "
				        "VectorType::CONSTANT_VECTOR; })) {\n"
				     << "ExecuteFlat(input, result);\n"
				     << "} else {\n"
				     << "ExecuteGeneric(input, result);\n"
				     << "}\n}\n";
			}

			expr.compiled_expression =
			    make_shared<CompiledExpression>(move(code).str(), "Execute", move(expression_info.input_expressions));
			return expr.compiled_expression->GetInputExpressions();
		}
	} catch (UnsupportedTypeException &) {
	}

	return expr.children;
}

//! Counter to help giving unique names to output files of the expression compiler
static int counter = 0;

CompiledExpression::CompiledExpression(string code, const string &function_name,
                                       vector<unique_ptr<Expression>> input_expressions)
    : input_expressions(move(input_expressions)) {
	auto start = high_resolution_clock::now();
	auto id = counter++;
	string base = EXPRESSION_COMPILER_OUTPUT_DIR;
	string code_file = base + "code" + to_string(id) + ".cpp";
	string compiled_code_file = base + "compiled" + to_string(id) + ".so";
	string error_messages_file = base + "error" + to_string(id) + ".txt";

	ofstream code_output {code_file};
	code_output << code;
	code_output.flush();

	std::ostringstream command_stream;
	command_stream << "clang++ -fPIC -rdynamic -O3 -DNDEBUG -shared -std=c++11 " << code_file << " -o "
	               << compiled_code_file << " " << DUCKDB_PATH_TO_LIBRARY << " > " << error_messages_file << " 2>&1";
	auto command = "bash -c \"" + move(command_stream).str() + "\"";
	auto result = system(command.c_str());
	if (result == -1) {
		std::perror("Could not compile expression");
		return;
	} else if (WIFEXITED(result) && WEXITSTATUS(result) != 0) {
		ifstream error {error_messages_file};
		if (error.is_open()) {
			std::cerr << "Could not compile expression: " << error.rdbuf() << "\n";
		}
		return;
	}

	handle = dlopen(compiled_code_file.c_str(), RTLD_NOW);
	if (!handle) {
		std::cerr << "error: " << dlerror() << "\n";
		return;
	}
	function = reinterpret_cast<function_t>(dlsym(handle, function_name.c_str()));
	auto stop = high_resolution_clock::now();
	milliseconds = duration_cast<std::chrono::milliseconds>(stop - start).count();
}

CompiledExpression::CompiledExpression(CompiledExpression &&other) noexcept
    : handle(other.handle), function(other.function), input_expressions(move(other.input_expressions)),
      milliseconds(other.milliseconds) {
	other.handle = nullptr;
}

CompiledExpression &CompiledExpression::operator=(CompiledExpression &&other) noexcept {
	if (handle && dlclose(handle)) {
		std::cerr << "Cannot close handle for compiled expression: " << dlerror() << "\n";
	}
	handle = other.handle;
	function = other.function;
	input_expressions = move(other.input_expressions);
	milliseconds = other.milliseconds;

	other.handle = nullptr;
	return *this;
}

CompiledExpression::~CompiledExpression() {
	if (handle && dlclose(handle)) {
		std::cerr << "Cannot close handle for compiled expression: " << dlerror() << "\n";
		return;
	}
}

static void CollectInputStates(vector<unique_ptr<ExpressionState>> &child_states,
                               vector<unique_ptr<ExpressionState>> &input_states) {
	for (auto &state : child_states) {
		if (state && state->expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			CollectInputStates(state->child_states, input_states);
		} else {
			input_states.emplace_back(move(state));
		}
	}
}

void CompiledExpression::Upgrade(BoundFunctionExpression &expr, ExpressionState &state, DataChunk &arguments) {
	D_ASSERT(IsReady());

	auto current_function = expr.function.function.target<function_t>();
	if (current_function && *current_function == function) {
		// Upgrade was already done before
		return;
	}

	vector<unique_ptr<ExpressionState>> input_states;
	CollectInputStates(state.child_states, input_states);

	// Reset state
	state.child_states.clear();
	state.types.clear();
	state.intermediate_chunk.Destroy();

	for (const auto &expr : input_expressions) {
		state.types.push_back(expr->return_type);
	}
	state.child_states = move(input_states);
	state.Finalize();

	if (!state.types.empty()) {
		arguments.InitializeEmpty(state.types);
	}

	// Upgrade expression
	expr.children = move(input_expressions);
	expr.function.function = function;
}

} // namespace duckdb

#else

namespace duckdb {

void ExpressionCompiler::CompileExpressions(PhysicalOperator &) {
}

uint64_t ExpressionCompiler::GetCompilationTime(PhysicalOperator &) {
	return 0;
}

CompiledExpression::CompiledExpression(string, const string &, vector<unique_ptr<Expression>>) {
}

CompiledExpression::CompiledExpression(CompiledExpression &&) noexcept {
}

CompiledExpression &CompiledExpression::operator=(CompiledExpression &&) noexcept {
	return *this;
}

CompiledExpression::~CompiledExpression() {
}

void CompiledExpression::Upgrade(BoundFunctionExpression &, ExpressionState &, DataChunk &) {
}

} // namespace duckdb

#endif