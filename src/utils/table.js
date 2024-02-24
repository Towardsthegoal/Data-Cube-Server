const dataTypeOrder = ['Text', 'Integer', 'Decimal', "Boolean", 'Date', 'TimeStamp'];

module.exports = function compareDataTypes(a, b) {
  const typeA = dataTypeOrder.indexOf(a.type);
  const typeB = dataTypeOrder.indexOf(b.type);
  
  return typeA - typeB;
};