sz = 5


all_sz = ord('z') - ord('a') + 1
part_sz = all_sz//sz + 1
ls = []
for i in range(ord('a'), ord('z')+1,part_sz):
    ls.append(chr(i))

if ls[-1] != 'z':
    ls.append('z')

ls = ls[1:]
print(ls)