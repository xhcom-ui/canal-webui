export function tablePagination(pageSize = 10) {
  return {
    defaultPageSize: pageSize,
    showSizeChanger: true,
    showQuickJumper: true,
    pageSizeOptions: ['10', '20', '50', '100'],
    showTotal: total => `共 ${total} 条`
  }
}
