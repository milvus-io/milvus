#include "prometheus/text_serializer.h"

#include <cmath>
#include <limits>
#include <ostream>

namespace prometheus {

namespace {

// Write a double as a string, with proper formatting for infinity and NaN
std::string ToString(double v) {
  if (std::isnan(v)) {
    return "Nan";
  }
  if (std::isinf(v)) {
    return (v < 0 ? "-Inf" : "+Inf");
  }
  return std::to_string(v);
}

const std::string& EscapeLabelValue(const std::string& value,
                                    std::string* tmp) {
  bool copy = false;
  for (size_t i = 0; i < value.size(); ++i) {
    auto c = value[i];
    if (c == '\\' || c == '"' || c == '\n') {
      if (!copy) {
        tmp->reserve(value.size() + 1);
        tmp->assign(value, 0, i);
        copy = true;
      }
      if (c == '\\') {
        tmp->append("\\\\");
      } else if (c == '"') {
        tmp->append("\\\"");
      } else {
        tmp->append("\\\n");
      }
    } else if (copy) {
      tmp->push_back(c);
    }
  }
  return copy ? *tmp : value;
}

// Write a line header: metric name and labels
void WriteHead(std::ostream& out, const MetricFamily& family,
               const ClientMetric& metric, const std::string& suffix = "",
               const std::string& extraLabelName = "",
               const std::string& extraLabelValue = "") {
  out << family.name << suffix;
  if (!metric.label.empty() || !extraLabelName.empty()) {
    out << "{";
    const char* prefix = "";
    std::string tmp;
    for (auto& lp : metric.label) {
      out << prefix << lp.name << "=\"" << EscapeLabelValue(lp.value, &tmp)
          << "\"";
      prefix = ",";
    }
    if (!extraLabelName.empty()) {
      out << prefix << extraLabelName << "=\""
          << EscapeLabelValue(extraLabelValue, &tmp) << "\"";
    }
    out << "}";
  }
  out << " ";
}

// Write a line trailer: timestamp
void WriteTail(std::ostream& out, const ClientMetric& metric) {
  if (metric.timestamp_ms != 0) {
    out << " " << metric.timestamp_ms;
  }
  out << "\n";
}

void SerializeCounter(std::ostream& out, const MetricFamily& family,
                      const ClientMetric& metric) {
  WriteHead(out, family, metric);
  out << ToString(metric.counter.value);
  WriteTail(out, metric);
}

void SerializeGauge(std::ostream& out, const MetricFamily& family,
                    const ClientMetric& metric) {
  WriteHead(out, family, metric);
  out << ToString(metric.gauge.value);
  WriteTail(out, metric);
}

void SerializeSummary(std::ostream& out, const MetricFamily& family,
                      const ClientMetric& metric) {
  auto& sum = metric.summary;
  WriteHead(out, family, metric, "_count");
  out << sum.sample_count;
  WriteTail(out, metric);

  WriteHead(out, family, metric, "_sum");
  out << ToString(sum.sample_sum);
  WriteTail(out, metric);

  for (auto& q : sum.quantile) {
    WriteHead(out, family, metric, "", "quantile", ToString(q.quantile));
    out << ToString(q.value);
    WriteTail(out, metric);
  }
}

void SerializeUntyped(std::ostream& out, const MetricFamily& family,
                      const ClientMetric& metric) {
  WriteHead(out, family, metric);
  out << ToString(metric.untyped.value);
  WriteTail(out, metric);
}

void SerializeHistogram(std::ostream& out, const MetricFamily& family,
                        const ClientMetric& metric) {
  auto& hist = metric.histogram;
  WriteHead(out, family, metric, "_count");
  out << hist.sample_count;
  WriteTail(out, metric);

  WriteHead(out, family, metric, "_sum");
  out << ToString(hist.sample_sum);
  WriteTail(out, metric);

  double last = -std::numeric_limits<double>::infinity();
  for (auto& b : hist.bucket) {
    WriteHead(out, family, metric, "_bucket", "le", ToString(b.upper_bound));
    last = b.upper_bound;
    out << b.cumulative_count;
    WriteTail(out, metric);
  }

  if (last != std::numeric_limits<double>::infinity()) {
    WriteHead(out, family, metric, "_bucket", "le", "+Inf");
    out << hist.sample_count;
    WriteTail(out, metric);
  }
}

void SerializeFamily(std::ostream& out, const MetricFamily& family) {
  if (!family.help.empty()) {
    out << "# HELP " << family.name << " " << family.help << "\n";
  }
  switch (family.type) {
    case MetricType::Counter:
      out << "# TYPE " << family.name << " counter\n";
      for (auto& metric : family.metric) {
        SerializeCounter(out, family, metric);
      }
      break;
    case MetricType::Gauge:
      out << "# TYPE " << family.name << " gauge\n";
      for (auto& metric : family.metric) {
        SerializeGauge(out, family, metric);
      }
      break;
    case MetricType::Summary:
      out << "# TYPE " << family.name << " summary\n";
      for (auto& metric : family.metric) {
        SerializeSummary(out, family, metric);
      }
      break;
    case MetricType::Untyped:
      out << "# TYPE " << family.name << " untyped\n";
      for (auto& metric : family.metric) {
        SerializeUntyped(out, family, metric);
      }
      break;
    case MetricType::Histogram:
      out << "# TYPE " << family.name << " histogram\n";
      for (auto& metric : family.metric) {
        SerializeHistogram(out, family, metric);
      }
      break;
    default:
      break;
  }
}
}  // namespace

void TextSerializer::Serialize(std::ostream& out,
                               const std::vector<MetricFamily>& metrics) const {
  for (auto& family : metrics) {
    SerializeFamily(out, family);
  }
}
}  // namespace prometheus
