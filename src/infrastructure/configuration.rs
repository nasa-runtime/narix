//! 保留 NARIX 字符串占位符和数字数组下标语义，求值与预算由 naml 承担。

use anyhow::{bail, Context};
use nasa::yml::strict::{
    self, ConfigPath, EnvironmentSnapshot, LoadPolicy, PathSegment, ValueHint,
};
use serde_json::Value;

/// 业务作用: 固定本轮实际使用的环境输入，避免无关变量进入配置快照或非法编码被静默忽略。
/// 参数说明: 无。
///
/// 返回: NARIX_* 与 RUST_LOG 的 UTF-8 键值；相关变量编码非法时拒绝加载，不展示变量内容。
pub(crate) fn environment_variables() -> anyhow::Result<Vec<(String, String)>> {
    let mut variables = Vec::new();
    for (key, value) in std::env::vars_os() {
        let Some(key) = key.to_str() else {
            continue;
        };
        if !key.starts_with("NARIX_") && key != "RUST_LOG" {
            continue;
        }
        let value = value
            .into_string()
            .map_err(|_| anyhow::anyhow!("NARIX configuration environment is not valid UTF-8"))?;
        variables.push((key.to_string(), value));
    }
    Ok(variables)
}

/// 业务作用: 为文件解析和表达式求值提供统一且有限的资源预算。
/// 参数说明: 无。
///
/// 返回: 禁止隐式环境回退、允许最大 16 MiB 来源的加载策略。
pub(crate) fn policy() -> LoadPolicy {
    let mut policy = LoadPolicy {
        environment_overlay: false,
        environment_fallback: false,
        ..LoadPolicy::default()
    };
    policy.limits.source_bytes = 16 * 1024 * 1024;
    policy
}

/// 业务作用: 将旧式数组引用规范化后交给 naml 求值，保留原字符串字段的类型。
/// 参数说明:
/// - `tree`: 已完成领域覆盖合并的配置树。
/// - `environment`: 本次加载固定的环境快照，日志仅允许访问明确的日志环境变量。
///
/// 返回: 求值成功后整体替换候选树；循环、缺失引用和超限输入拒绝发布。
pub(crate) fn resolve(tree: &mut Value, environment: &EnvironmentSnapshot) -> anyhow::Result<()> {
    let mut normalized = tree.clone();
    let mut literal_key = "__narix_reference_dollar".to_string();
    while tree.get(&literal_key).is_some() {
        literal_key.push('_');
    }
    normalize(&mut normalized, tree, &ConfigPath::default(), &literal_key)?;
    // 旧配置中的前导 $ 是正文；以独立标量引用隔开，避免被 naml 解释成转义操作符。
    normalized
        .as_object_mut()
        .context("configuration root must be an object")?
        .insert(literal_key.clone(), Value::String("$".to_string()));
    let mut policy = policy();
    // 只有新增日志配置采用 NASA 的环境默认值和嵌套表达式，旧业务引用仍经过路径准入。
    policy.environment_fallback = true;
    policy
        .hints
        .insert(ConfigPath::parse("log.level")?, ValueHint::String);
    if tree
        .get("log")
        .and_then(|log| log.get("path"))
        .is_some_and(Value::is_string)
    {
        policy
            .hints
            .insert(ConfigPath::parse("log.path")?, ValueHint::String);
    }
    let environment = environment.clone().allow_fallback(
        ["NARIX_LOG_LEVEL".to_string(), "NARIX_LOG_PATH".to_string()]
            .into_iter()
            .collect(),
    );
    let mut resolved = strict::resolve(&normalized, &environment, &policy)?.tree;
    resolved
        .as_object_mut()
        .context("resolved configuration root must be an object")?
        .remove(&literal_key);
    retain_string_types(&mut resolved, tree);
    *tree = resolved;
    Ok(())
}

/// 业务作用: 仅转换引用路径语法，不承担依赖展开或循环判定。
/// 参数说明:
/// - `value`: 待转换的候选子树。
/// - `root`: 转换前的完整配置，决定数字段是对象键还是数组下标。
/// - `location`: 当前字段位置，用于无正文错误定位。
/// - `literal_key`: 本轮专用的美元符号标量路径，不覆盖任何用户字段。
///
/// 返回: 引用均为原配置支持的标量路径时成功。
fn normalize(
    value: &mut Value,
    root: &Value,
    location: &ConfigPath,
    literal_key: &str,
) -> anyhow::Result<()> {
    if matches!(location.0.first(), Some(PathSegment::Key(key)) if key == "log") {
        return Ok(());
    }
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                normalize(child, root, &location.key(key), literal_key)?;
            }
        }
        Value::Array(values) => {
            for (index, child) in values.iter_mut().enumerate() {
                normalize(child, root, &location.index(index), literal_key)?;
            }
        }
        Value::String(text) => {
            let mut output = String::with_capacity(text.len());
            let mut remaining = text.as_str();
            while let Some(start) = remaining.find("${") {
                let prefix = &remaining[..start];
                let literal = prefix.trim_end_matches('$');
                output.push_str(literal);
                for _ in literal.len()..prefix.len() {
                    output.push_str("${");
                    output.push_str(literal_key);
                    output.push('}');
                }
                let expression = &remaining[start + 2..];
                let end = expression
                    .find('}')
                    .with_context(|| format!("unclosed configuration reference at {location}"))?;
                let reference = expression[..end].trim();
                let mut current = root;
                let mut canonical = String::new();
                let mut identity = ConfigPath::default();
                for segment in reference.split('.') {
                    match current {
                        Value::Object(map) => {
                            if !canonical.is_empty() {
                                canonical.push('.');
                            }
                            canonical.push_str(segment);
                            identity = identity.key(segment);
                            current = map.get(segment).with_context(|| {
                                format!("missing configuration reference at {location}")
                            })?;
                        }
                        Value::Array(items) => {
                            let index: usize = segment.parse().with_context(|| {
                                format!("invalid array reference at {location}")
                            })?;
                            canonical.push_str(&format!("[{index}]"));
                            identity = identity.index(index);
                            current = items.get(index).with_context(|| {
                                format!("missing array reference at {location}")
                            })?;
                        }
                        _ => bail!("configuration reference crosses a scalar at {location}"),
                    }
                }
                if !matches!(
                    current,
                    Value::String(_) | Value::Bool(_) | Value::Number(_)
                ) {
                    bail!("configuration reference requires a scalar at {location}");
                }
                let parsed = ConfigPath::parse(&canonical)?;
                if parsed != identity {
                    bail!("ambiguous configuration reference at {location}");
                }
                output.push_str("${");
                output.push_str(&canonical);
                output.push('}');
                remaining = &expression[end + 1..];
            }
            output.push_str(remaining);
            *text = output;
        }
        _ => {}
    }
    Ok(())
}

/// 业务作用: 保持 NARIX 原有整值占位符也产生字符串的配置合同。
/// 参数说明:
/// - `resolved`: naml 完成求值的配置树。
/// - `original`: 求值前的字段类型。
///
/// 返回: 原字符串引用的数字和布尔值转换成文本，原数值字段保持原类型。
fn retain_string_types(resolved: &mut Value, original: &Value) {
    match (resolved, original) {
        (Value::Object(values), Value::Object(original)) => {
            for (key, value) in values {
                if let Some(before) = original.get(key) {
                    retain_string_types(value, before);
                }
            }
        }
        (Value::Array(values), Value::Array(original)) => {
            for (value, before) in values.iter_mut().zip(original) {
                retain_string_types(value, before);
            }
        }
        (value, Value::String(_)) if !value.is_string() => {
            *value = Value::String(value.to_string());
        }
        _ => {}
    }
}
