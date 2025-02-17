def bin_search(arr, x, neighbor=None):
    left = 0
    right = len(arr) - 1
    # 基本判断
    while right >= left:
        mid = int(left + (right - left)/2)
        # 元素整好的中间位置
        if arr[mid] == x:
            return mid
        # 元素小于中间位置的元素，只需要再比较左边的元素
        elif arr[mid] > x:
            right = mid - 1
        # 元素大于中间位置的元素，只需要再比较右边的元素
        else:
            left = mid + 1
    else:
        if neighbor is None:
            return -1
        elif neighbor == 'small':
            return min(left, right)
        elif neighbor == 'large':
            p = max(left, right)
            if p >= len(arr):
                return -1
            else:
                return p
        return -1

