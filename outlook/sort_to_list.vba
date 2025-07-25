Sub SortToRecipients()
    Dim Mail As MailItem
    Dim Recipients As Outlook.Recipients
    Dim AddrList() As String
    Dim i As Integer

    If Application.ActiveInspector.CurrentItem.Class = olMail Then
        Set Mail = Application.ActiveInspector.CurrentItem
        Set Recipients = Mail.Recipients

        If Recipients.Count > 1 Then
            ReDim AddrList(Recipients.Count - 1)
            For i = 1 To Recipients.Count
                AddrList(i - 1) = Recipients.Item(i).Address
            Next i

            ' Sort alphabetically
            Call BubbleSort(AddrList)

            ' Clear and re-add
            Mail.To = ""
            For i = 0 To UBound(AddrList)
                Mail.To = Mail.To & AddrList(i) & "; "
            Next i
        End If
    End If
End Sub

Sub BubbleSort(arr() As String)
    Dim i As Long, j As Long
    Dim temp As String
    For i = LBound(arr) To UBound(arr) - 1
        For j = i + 1 To UBound(arr)
            If LCase(arr(i)) > LCase(arr(j)) Then
                temp = arr(i)
                arr(i) = arr(j)
                arr(j) = temp
            End If
        Next j
    Next i
End Sub
